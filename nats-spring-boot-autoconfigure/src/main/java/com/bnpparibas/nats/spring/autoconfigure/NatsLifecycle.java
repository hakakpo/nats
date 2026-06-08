package com.bnpparibas.nats.spring.autoconfigure;

import com.bnpparibas.nats.core.NatsConsumerDefinition;
import com.bnpparibas.nats.core.NatsStreamDefinition;
import com.bnpparibas.nats.core.NatsValidationReport;
import com.bnpparibas.nats.core.NatsConnectionManager;
import com.bnpparibas.nats.core.NatsTopologyOperations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;

import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;

class NatsLifecycle implements SmartLifecycle {
    private static final Logger LOGGER = LoggerFactory.getLogger(NatsLifecycle.class);

    private final BnppNatsProperties properties;
    private final NatsConnectionManager connectionManager;
    private final NatsTopologyOperations topologyOperations;
    private final AtomicBoolean running = new AtomicBoolean(false);

    NatsLifecycle(BnppNatsProperties properties, NatsConnectionManager connectionManager, NatsTopologyOperations topologyOperations) {
        this.properties = properties;
        this.connectionManager = connectionManager;
        this.topologyOperations = topologyOperations;
    }

    @Override
    public void start() {
        if (!properties.isAutoStart() || !running.compareAndSet(false, true)) {
            return;
        }
        CompletionStage<?> startup = connectionManager.connectAsync()
                .thenCompose(ignored -> applyTopology())
                .whenComplete((ignored, error) -> {
                    if (error != null) {
                        running.set(false);
                        LOGGER.error("NATS startup failed", error);
                    }
                });
        if (properties.isFailOnInvalidTopology()) {
            startup.toCompletableFuture().join();
        }
    }

    @Override
    public void stop() {
        running.set(false);
        connectionManager.closeAsync().toCompletableFuture().join();
    }

    @Override
    public boolean isRunning() {
        return running.get();
    }

    @Override
    public int getPhase() {
        return Integer.MAX_VALUE - 100;
    }

    private CompletionStage<?> applyTopology() {
        List<NatsStreamDefinition> streams = properties.getTopology().getStreams().stream()
                .map(this::toStreamDefinition)
                .toList();
        List<NatsConsumerDefinition> consumers = properties.getTopology().getConsumers().stream()
                .map(this::toConsumerDefinition)
                .toList();

        CompletionStage<?> stage = connectionManager.connection();
        if (properties.isCreateTopology()) {
            for (NatsStreamDefinition stream : streams) {
                stage = stage.thenCompose(ignored -> topologyOperations.ensureStream(stream));
            }
            for (NatsConsumerDefinition consumer : consumers) {
                stage = stage.thenCompose(ignored -> topologyOperations.ensureConsumer(consumer));
            }
        }
        if (properties.isValidateTopology()) {
            stage = stage.thenCompose(ignored -> topologyOperations.validate(streams, consumers))
                    .thenAccept(this::handleValidationReport);
        }
        return stage;
    }

    private void handleValidationReport(NatsValidationReport report) {
        if (report.valid()) {
            return;
        }
        String message = "Invalid NATS topology: " + report.errors();
        if (properties.isFailOnInvalidTopology()) {
            throw new IllegalStateException(message);
        }
        LOGGER.warn(message);
    }

    private NatsStreamDefinition toStreamDefinition(BnppNatsProperties.Stream stream) {
        return new NatsStreamDefinition(
                stream.getName(),
                stream.getSubjects(),
                stream.getStorageType(),
                stream.getRetentionPolicy(),
                stream.getReplicas(),
                stream.getMaxMessages(),
                stream.getMaxBytes(),
                stream.getMaxAge(),
                stream.getDuplicateWindow(),
                stream.getMetadata());
    }

    private NatsConsumerDefinition toConsumerDefinition(BnppNatsProperties.Consumer consumer) {
        return new NatsConsumerDefinition(
                consumer.getStream(),
                consumer.getDurable(),
                consumer.getName(),
                consumer.getFilterSubjects(),
                consumer.getAckPolicy(),
                consumer.getDeliverPolicy(),
                consumer.getAckWait(),
                consumer.getMaxDeliver(),
                consumer.getMaxAckPending(),
                consumer.getMetadata());
    }
}
