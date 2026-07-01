package com.bnpparibas.nats.core;

import io.nats.client.JetStreamManagement;
import io.nats.client.api.AckPolicy;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.api.DeliverPolicy;
import io.nats.client.api.RetentionPolicy;
import io.nats.client.api.StorageType;
import io.nats.client.api.StreamConfiguration;
import io.nats.client.api.StreamInfo;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
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
                Map<String, NatsStreamDefinition> streamsByName = streams.stream()
                        .collect(Collectors.toMap(NatsStreamDefinition::name, Function.identity(), (left, right) -> left));
                for (NatsStreamDefinition expected : streams) {
                    validateStream(management, expected, errors, warnings);
                }
                for (NatsConsumerDefinition expected : consumers) {
                    validateConsumer(management, expected, errors);
                    validateExactOnceConsumer(management, streamsByName, expected, errors);
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
            ConsumerConfiguration actual = management.getConsumerInfo(expected.stream(), expected.durable()).getConsumerConfiguration();
            if (expected.deliverGroup() != null && !expected.deliverGroup().isBlank()
                    && !expected.deliverGroup().equals(actual.getDeliverGroup())) {
                errors.add("Consumer " + expected.durable() + " on stream " + expected.stream()
                        + " deliver group differs from expected value " + expected.deliverGroup());
            }
        } catch (Exception ex) {
            errors.add("Consumer " + expected.durable() + " on stream " + expected.stream() + " is missing or unreadable: " + ex.getMessage());
        }
    }

    private void validateExactOnceConsumer(
            JetStreamManagement management,
            Map<String, NatsStreamDefinition> streamsByName,
            NatsConsumerDefinition expected,
            List<String> errors) {
        if (!expected.exactOnceProcessing()) {
            return;
        }
        if (!isExplicitAckPolicy(expected.ackPolicy())) {
            errors.add("Consumer " + expected.durable() + " on stream " + expected.stream()
                    + " has exact-once-processing enabled but ack-policy is not Explicit");
        }
        if (!streamHasDuplicateWindow(management, streamsByName.get(expected.stream()), expected.stream())) {
            errors.add("Consumer " + expected.durable() + " on stream " + expected.stream()
                    + " has exact-once-processing enabled but the stream has no duplicate-window");
        }
    }

    private boolean isExplicitAckPolicy(String ackPolicy) {
        return ackPolicy != null && AckPolicy.Explicit.toString().equalsIgnoreCase(ackPolicy);
    }

    private boolean streamHasDuplicateWindow(
            JetStreamManagement management,
            NatsStreamDefinition configuredStream,
            String streamName) {
        if (configuredStream != null && hasDuration(configuredStream.duplicateWindow())) {
            return true;
        }
        try {
            return hasDuration(management.getStreamInfo(streamName).getConfiguration().getDuplicateWindow());
        } catch (Exception ignored) {
            return false;
        }
    }

    private boolean hasDuration(Duration duration) {
        return duration != null && !duration.isZero() && !duration.isNegative();
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
        if (consumer.deliverGroup() != null && !consumer.deliverGroup().isBlank()) {
            builder.deliverGroup(consumer.deliverGroup());
        }
        if (!consumer.metadata().isEmpty()) {
            builder.metadata(consumer.metadata());
        }
        return builder.build();
    }
}
