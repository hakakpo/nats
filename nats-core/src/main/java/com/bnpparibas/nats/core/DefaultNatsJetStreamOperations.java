package com.bnpparibas.nats.core;

import io.nats.client.Dispatcher;
import io.nats.client.Connection;
import io.nats.client.JetStream;
import io.nats.client.JetStreamApiException;
import io.nats.client.JetStreamManagement;
import io.nats.client.JetStreamSubscription;
import io.nats.client.PublishOptions;
import io.nats.client.PullSubscribeOptions;
import io.nats.client.PushSubscribeOptions;
import io.nats.client.api.AckPolicy;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.api.PublishAck;
import io.nats.client.api.RetentionPolicy;
import io.nats.client.api.StorageType;
import io.nats.client.api.StreamConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;

public class DefaultNatsJetStreamOperations implements NatsJetStreamOperations {
    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultNatsJetStreamOperations.class);

    private final NatsConnectionManager connectionManager;
    private final ExecutorService executor;
    private final boolean autoCreateStreamOnPublishFailure;

    public DefaultNatsJetStreamOperations(NatsConnectionManager connectionManager, ExecutorService executor) {
        this(connectionManager, executor, false);
    }

    public DefaultNatsJetStreamOperations(
            NatsConnectionManager connectionManager,
            ExecutorService executor,
            boolean autoCreateStreamOnPublishFailure) {
        this.connectionManager = connectionManager;
        this.executor = executor;
        this.autoCreateStreamOnPublishFailure = autoCreateStreamOnPublishFailure;
    }

    @Override
    public CompletionStage<NatsJetStreamPublishAck> publish(NatsJetStreamPublishRequest request) {
        return connectionManager.connection()
                .thenCompose(connection -> publishWithStreamRecovery(connection, request))
                .thenApply(this::toAck);
    }

    private CompletionStage<PublishAck> publishWithStreamRecovery(Connection connection, NatsJetStreamPublishRequest request) {
        return publishAsync(connection, request)
                .handle((ack, error) -> {
                    if (error == null) {
                        return CompletableFuture.completedFuture(ack);
                    }
                    return recoverMissingStreamAndRepublish(connection, request, error);
                })
                .thenCompose(stage -> stage);
    }

    private CompletionStage<PublishAck> publishAsync(Connection connection, NatsJetStreamPublishRequest request) {
        try {
            JetStream jetStream = connection.jetStream();
            PublishOptions options = publishOptions(request);
            return jetStream.publishAsync(
                    request.subject(),
                    NatsHeaderMapper.toNatsHeaders(request.headers()),
                    request.data(),
                    options);
        } catch (Exception ex) {
            return CompletableFuture.failedFuture(new NatsClientException("Unable to publish to JetStream", ex));
        }
    }

    private CompletionStage<PublishAck> recoverMissingStreamAndRepublish(
            Connection connection,
            NatsJetStreamPublishRequest request,
            Throwable error) {
        if (!autoCreateStreamOnPublishFailure || !isMissingStreamPublishFailure(error)) {
            return CompletableFuture.failedFuture(error);
        }

        String streamName = streamName(request);
        if (streamName == null || streamName.isBlank()) {
            return CompletableFuture.failedFuture(new NatsClientException(
                    "Unable to auto-create JetStream stream after publish failure: stream or expectedStream is required",
                    unwrap(error)));
        }

        return CompletableFuture.runAsync(() -> createStreamAfterPublishFailure(connection, streamName, request.subject()), executor)
                .thenCompose(ignored -> publishAsync(connection, request));
    }

    private void createStreamAfterPublishFailure(Connection connection, String streamName, String subject) {
        try {
            JetStreamManagement management = connection.jetStreamManagement();
            StreamConfiguration configuration = StreamConfiguration.builder()
                    .name(streamName)
                    .subjects(subject)
                    .storageType(StorageType.File)
                    .retentionPolicy(RetentionPolicy.Limits)
                    .build();
            management.addStream(configuration);
            LOGGER.info("Created NATS JetStream stream '{}' for subject '{}' after publish failure", streamName, subject);
        } catch (Exception createError) {
            if (streamAlreadyExists(connection, streamName)) {
                LOGGER.debug("NATS JetStream stream '{}' already exists while recovering from publish failure", streamName);
                return;
            }
            throw new NatsClientException("Unable to auto-create JetStream stream " + streamName, createError);
        }
    }

    private boolean streamAlreadyExists(Connection connection, String streamName) {
        try {
            connection.jetStreamManagement().getStreamInfo(streamName);
            return true;
        } catch (Exception ignored) {
            return false;
        }
    }

    private String streamName(NatsJetStreamPublishRequest request) {
        if (request.stream() != null && !request.stream().isBlank()) {
            return request.stream();
        }
        return request.expectedStream();
    }

    private boolean isMissingStreamPublishFailure(Throwable error) {
        Throwable current = unwrap(error);
        while (current != null) {
            if (current instanceof JetStreamApiException jetStreamError && containsMissingStreamSignal(jetStreamError.getErrorDescription())) {
                return true;
            }
            if (containsMissingStreamSignal(current.getMessage())) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }

    private boolean containsMissingStreamSignal(String message) {
        if (message == null) {
            return false;
        }
        String normalized = message.toLowerCase();
        return normalized.contains("no stream")
                || normalized.contains("stream not found")
                || normalized.contains("no matching stream")
                || normalized.contains("no response from stream")
                || normalized.contains("no responders");
    }

    private Throwable unwrap(Throwable error) {
        Throwable current = error;
        while ((current instanceof CompletionException || current instanceof ExecutionException) && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    @Override
    public CompletionStage<NatsSubscriptionHandle> subscribePush(NatsJetStreamPushSubscribeRequest request, NatsMessageHandler handler) {
        return connectionManager.connection().thenApplyAsync(connection -> {
            try {
                JetStream jetStream = connection.jetStream();
                Dispatcher dispatcher = connection.createDispatcher();
                io.nats.client.MessageHandler natsHandler = msg -> handler.onMessage(new NatsReceivedMessage(msg, executor));
                PushSubscribeOptions options = pushSubscribeOptions(request);
                JetStreamSubscription subscription;
                if (request.queueGroup() == null || request.queueGroup().isBlank()) {
                    subscription = jetStream.subscribe(request.subject(), dispatcher, natsHandler, request.autoAck(), options);
                } else {
                    subscription = jetStream.subscribe(request.subject(), request.queueGroup(), dispatcher, natsHandler, request.autoAck(), options);
                }
                return new JetStreamDispatcherSubscriptionHandle(
                        connection,
                        dispatcher,
                        subscription,
                        request.subject(),
                        request.queueGroup(),
                        executor);
            } catch (Exception ex) {
                throw new NatsClientException("Unable to subscribe to JetStream", ex);
            }
        }, executor);
    }

    @Override
    public CompletionStage<List<NatsReceivedMessage>> fetch(NatsJetStreamPullFetchRequest request) {
        return connectionManager.connection().thenApplyAsync(connection -> {
            try {
                JetStream jetStream = connection.jetStream();
                PullSubscribeOptions options = PullSubscribeOptions.builder()
                        .stream(request.stream())
                        .durable(request.durable())
                        .build();
                JetStreamSubscription subscription = jetStream.subscribe(request.subject(), options);
                try {
                    return subscription.fetch(request.batchSize(), request.expiresIn())
                            .stream()
                            .map(message -> new NatsReceivedMessage(message, executor))
                            .toList();
                } finally {
                    subscription.unsubscribe();
                }
            } catch (Exception ex) {
                throw new NatsClientException("Unable to fetch from JetStream", ex);
            }
        }, executor);
    }

    private PublishOptions publishOptions(NatsJetStreamPublishRequest request) {
        PublishOptions.Builder builder = PublishOptions.builder();
        if (request.stream() != null) {
            builder.stream(request.stream());
        }
        if (request.expectedStream() != null) {
            builder.expectedStream(request.expectedStream());
        }
        if (request.messageId() != null) {
            builder.messageId(request.messageId());
        }
        if (request.expectedLastSequence() != null) {
            builder.expectedLastSequence(request.expectedLastSequence());
        }
        if (request.expectedLastMessageId() != null) {
            builder.expectedLastMsgId(request.expectedLastMessageId());
        }
        if (request.streamTimeout() != null) {
            builder.streamTimeout(request.streamTimeout());
        }
        return builder.build();
    }

    private PushSubscribeOptions pushSubscribeOptions(NatsJetStreamPushSubscribeRequest request) {
        ConsumerConfiguration.Builder consumer = ConsumerConfiguration.builder().ackPolicy(AckPolicy.Explicit);
        if (request.durable() != null) {
            consumer.durable(request.durable());
        }
        if (request.name() != null) {
            consumer.name(request.name());
        }
        if (!request.filterSubjects().isEmpty()) {
            consumer.filterSubjects(request.filterSubjects());
        }
        if (request.ackWait() != null) {
            consumer.ackWait(request.ackWait());
        }
        if (request.maxAckPending() != null) {
            consumer.maxAckPending(request.maxAckPending());
        }
        PushSubscribeOptions.Builder builder = PushSubscribeOptions.builder()
                .configuration(consumer.build())
                .ordered(request.ordered());
        if (request.stream() != null) {
            builder.stream(request.stream());
        }
        if (request.durable() != null) {
            builder.durable(request.durable());
        }
        return builder.build();
    }

    private NatsJetStreamPublishAck toAck(PublishAck ack) {
        return new NatsJetStreamPublishAck(ack.getStream(), ack.getSeqno(), ack.isDuplicate(), ack.getDomain());
    }

    private record JetStreamDispatcherSubscriptionHandle(
            io.nats.client.Connection connection,
            Dispatcher dispatcher,
            JetStreamSubscription subscription,
            String subject,
            String queueGroup,
            ExecutorService executor
    ) implements NatsSubscriptionHandle {
        @Override
        public CompletionStage<Void> stopAsync() {
            return CompletableFuture.runAsync(() -> {
                subscription.unsubscribe();
                connection.closeDispatcher(dispatcher);
            }, executor);
        }
    }
}
