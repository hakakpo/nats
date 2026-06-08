package com.bnpparibas.nats.core;

import io.nats.client.JetStreamManagement;
import io.nats.client.api.AckPolicy;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.api.DeliverPolicy;
import io.nats.client.api.RetentionPolicy;
import io.nats.client.api.StorageType;
import io.nats.client.api.StreamConfiguration;
import io.nats.client.api.StreamInfo;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;

public class DefaultNatsTopologyOperations implements NatsTopologyOperations {
    private final NatsConnectionManager connectionManager;
    private final ExecutorService executor;

    public DefaultNatsTopologyOperations(NatsConnectionManager connectionManager, ExecutorService executor) {
        this.connectionManager = connectionManager;
        this.executor = executor;
    }

    @Override
    public CompletionStage<Void> ensureStream(NatsStreamDefinition stream) {
        return connectionManager.connection().thenAcceptAsync(connection -> {
            try {
                JetStreamManagement management = connection.jetStreamManagement();
                StreamConfiguration configuration = toStreamConfiguration(stream);
                try {
                    management.getStreamInfo(stream.name());
                    management.updateStream(configuration);
                } catch (Exception notFound) {
                    management.addStream(configuration);
                }
            } catch (Exception ex) {
                throw new NatsClientException("Unable to ensure stream " + stream.name(), ex);
            }
        }, executor);
    }

    @Override
    public CompletionStage<Void> ensureConsumer(NatsConsumerDefinition consumer) {
        return connectionManager.connection().thenAcceptAsync(connection -> {
            try {
                JetStreamManagement management = connection.jetStreamManagement();
                management.addOrUpdateConsumer(consumer.stream(), toConsumerConfiguration(consumer));
            } catch (Exception ex) {
                throw new NatsClientException("Unable to ensure consumer " + consumer.durable(), ex);
            }
        }, executor);
    }

    @Override
    public CompletionStage<NatsValidationReport> validate(List<NatsStreamDefinition> streams, List<NatsConsumerDefinition> consumers) {
        return connectionManager.connection().thenApplyAsync(connection -> {
            List<String> errors = new ArrayList<>();
            List<String> warnings = new ArrayList<>();
            try {
                JetStreamManagement management = connection.jetStreamManagement();
                for (NatsStreamDefinition expected : streams) {
                    validateStream(management, expected, errors, warnings);
                }
                for (NatsConsumerDefinition expected : consumers) {
                    validateConsumer(management, expected, errors);
                }
                return new NatsValidationReport(errors.isEmpty(), errors, warnings);
            } catch (Exception ex) {
                throw new NatsClientException("Unable to validate NATS topology", ex);
            }
        }, executor);
    }

    private void validateStream(JetStreamManagement management, NatsStreamDefinition expected, List<String> errors, List<String> warnings) {
        try {
            StreamInfo info = management.getStreamInfo(expected.name());
            List<String> actualSubjects = info.getConfiguration().getSubjects();
            if (!actualSubjects.containsAll(expected.subjects())) {
                errors.add("Stream " + expected.name() + " does not contain expected subjects " + expected.subjects());
            }
            if (expected.replicas() != null && info.getConfiguration().getReplicas() != expected.replicas()) {
                warnings.add("Stream " + expected.name() + " replicas differ from expected value " + expected.replicas());
            }
        } catch (Exception ex) {
            errors.add("Stream " + expected.name() + " is missing or unreadable: " + ex.getMessage());
        }
    }

    private void validateConsumer(JetStreamManagement management, NatsConsumerDefinition expected, List<String> errors) {
        try {
            management.getConsumerInfo(expected.stream(), expected.durable());
        } catch (Exception ex) {
            errors.add("Consumer " + expected.durable() + " on stream " + expected.stream() + " is missing or unreadable: " + ex.getMessage());
        }
    }

    private StreamConfiguration toStreamConfiguration(NatsStreamDefinition stream) {
        StreamConfiguration.Builder builder = StreamConfiguration.builder()
                .name(stream.name())
                .subjects(stream.subjects());
        if (stream.storageType() != null) {
            builder.storageType(StorageType.get(stream.storageType()));
        }
        if (stream.retentionPolicy() != null) {
            builder.retentionPolicy(RetentionPolicy.get(stream.retentionPolicy()));
        }
        if (stream.replicas() != null) {
            builder.replicas(stream.replicas());
        }
        if (stream.maxMessages() != null) {
            builder.maxMessages(stream.maxMessages());
        }
        if (stream.maxBytes() != null) {
            builder.maxBytes(stream.maxBytes());
        }
        if (stream.maxAge() != null) {
            builder.maxAge(stream.maxAge());
        }
        if (stream.duplicateWindow() != null) {
            builder.duplicateWindow(stream.duplicateWindow());
        }
        if (!stream.metadata().isEmpty()) {
            builder.metadata(stream.metadata());
        }
        return builder.build();
    }

    private ConsumerConfiguration toConsumerConfiguration(NatsConsumerDefinition consumer) {
        ConsumerConfiguration.Builder builder = ConsumerConfiguration.builder()
                .durable(consumer.durable())
                .name(consumer.name() == null ? consumer.durable() : consumer.name());
        if (!consumer.filterSubjects().isEmpty()) {
            builder.filterSubjects(consumer.filterSubjects());
        }
        if (consumer.ackPolicy() != null) {
            builder.ackPolicy(AckPolicy.get(consumer.ackPolicy()));
        }
        if (consumer.deliverPolicy() != null) {
            builder.deliverPolicy(DeliverPolicy.get(consumer.deliverPolicy()));
        }
        if (consumer.ackWait() != null) {
            builder.ackWait(consumer.ackWait());
        }
        if (consumer.maxDeliver() != null) {
            builder.maxDeliver(consumer.maxDeliver());
        }
        if (consumer.maxAckPending() != null) {
            builder.maxAckPending(consumer.maxAckPending());
        }
        if (!consumer.metadata().isEmpty()) {
            builder.metadata(consumer.metadata());
        }
        return builder.build();
    }
}
