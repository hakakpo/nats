package com.bnpparibas.nats.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.nats.client.Connection;
import io.nats.client.JetStreamManagement;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.api.ConsumerInfo;
import io.nats.client.api.StreamConfiguration;
import io.nats.client.api.StreamInfo;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

class DefaultNatsTopologyOperationsTest {

    @Test
    void ensureStreamAddsMissingStreamAndUpdatesExistingStream() throws Exception {
        Connection connection = mock(Connection.class);
        JetStreamManagement management = mock(JetStreamManagement.class);
        when(connection.jetStreamManagement()).thenReturn(management);
        StreamInfo existing = mock(StreamInfo.class);
        when(management.getStreamInfo("EVENTS"))
                .thenThrow(new RuntimeException("missing"))
                .thenReturn(existing);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            DefaultNatsTopologyOperations operations = new DefaultNatsTopologyOperations(new StaticConnectionManager(connection), executor);
            NatsStreamDefinition stream = new NatsStreamDefinition(
                    "EVENTS", List.of("events.>"), "File", "Limits", 1, 100L, 1024L,
                    Duration.ofHours(1), Duration.ofMinutes(2), Map.of("owner", "platform"));

            operations.ensureStream(stream).toCompletableFuture().join();
            operations.ensureStream(stream).toCompletableFuture().join();

            verify(management).addStream(any(StreamConfiguration.class));
            verify(management).updateStream(any(StreamConfiguration.class));
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void ensureConsumerAddsOrUpdatesConsumer() throws Exception {
        Connection connection = mock(Connection.class);
        JetStreamManagement management = mock(JetStreamManagement.class);
        when(connection.jetStreamManagement()).thenReturn(management);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            DefaultNatsTopologyOperations operations = new DefaultNatsTopologyOperations(new StaticConnectionManager(connection), executor);
            NatsConsumerDefinition consumer = new NatsConsumerDefinition(
                    "EVENTS", "worker", "worker", List.of("events.created"), "Explicit", "All",
                    Duration.ofSeconds(30), 5L, 100L, Map.of("owner", "platform"));

            operations.ensureConsumer(consumer).toCompletableFuture().join();

            ArgumentCaptor<ConsumerConfiguration> consumerCaptor = ArgumentCaptor.forClass(ConsumerConfiguration.class);
            verify(management).addOrUpdateConsumer(eq("EVENTS"), consumerCaptor.capture());
            assertThat(consumerCaptor.getValue().getDeliverGroup()).isNull();
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void ensureConsumerSetsDeliverGroupWhenConfigured() throws Exception {
        Connection connection = mock(Connection.class);
        JetStreamManagement management = mock(JetStreamManagement.class);
        when(connection.jetStreamManagement()).thenReturn(management);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            DefaultNatsTopologyOperations operations = new DefaultNatsTopologyOperations(new StaticConnectionManager(connection), executor);
            NatsConsumerDefinition consumer = new NatsConsumerDefinition(
                    "EVENTS", "worker", "worker", List.of("events.created"), "Explicit", "All",
                    Duration.ofSeconds(30), 5L, 100L, "workers", true, Map.of("owner", "platform"));

            operations.ensureConsumer(consumer).toCompletableFuture().join();

            ArgumentCaptor<ConsumerConfiguration> consumerCaptor = ArgumentCaptor.forClass(ConsumerConfiguration.class);
            verify(management).addOrUpdateConsumer(eq("EVENTS"), consumerCaptor.capture());
            assertThat(consumerCaptor.getValue().getDeliverGroup()).isEqualTo("workers");
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void validateReportsMissingResourcesAndWarnings() throws Exception {
        Connection connection = mock(Connection.class);
        JetStreamManagement management = mock(JetStreamManagement.class);
        when(connection.jetStreamManagement()).thenReturn(management);
        StreamInfo streamInfo = mock(StreamInfo.class);
        StreamConfiguration streamConfiguration = StreamConfiguration.builder()
                .name("EVENTS")
                .subjects("events.created")
                .replicas(1)
                .build();
        when(streamInfo.getConfiguration()).thenReturn(streamConfiguration);
        when(management.getStreamInfo("EVENTS")).thenReturn(streamInfo);
        ConsumerInfo consumerInfo = mockConsumerInfo(ConsumerConfiguration.builder().durable("worker").build());
        when(management.getConsumerInfo("EVENTS", "worker")).thenReturn(consumerInfo);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            DefaultNatsTopologyOperations operations = new DefaultNatsTopologyOperations(new StaticConnectionManager(connection), executor);
            NatsStreamDefinition stream = new NatsStreamDefinition("EVENTS", List.of("events.created"), null, null, 3, null, null, null, null, Map.of());
            NatsConsumerDefinition consumer = new NatsConsumerDefinition("EVENTS", "worker", null, List.of(), null, null, null, null, null, Map.of());

            NatsValidationReport report = operations.validate(List.of(stream), List.of(consumer)).toCompletableFuture().join();

            assertThat(report.valid()).isTrue();
            assertThat(report.warnings()).hasSize(1);
            assertThat(report.errors()).isEmpty();
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void validateReportsErrorsWhenExactOnceProcessingContractIsInvalid() throws Exception {
        Connection connection = mock(Connection.class);
        JetStreamManagement management = mock(JetStreamManagement.class);
        when(connection.jetStreamManagement()).thenReturn(management);
        StreamInfo streamInfo = mock(StreamInfo.class);
        StreamConfiguration streamConfiguration = StreamConfiguration.builder()
                .name("EVENTS")
                .subjects("events.created")
                .build();
        when(streamInfo.getConfiguration()).thenReturn(streamConfiguration);
        when(management.getStreamInfo("EVENTS")).thenReturn(streamInfo);
        ConsumerInfo consumerInfo = mockConsumerInfo(ConsumerConfiguration.builder().durable("worker").build());
        when(management.getConsumerInfo("EVENTS", "worker")).thenReturn(consumerInfo);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            DefaultNatsTopologyOperations operations = new DefaultNatsTopologyOperations(new StaticConnectionManager(connection), executor);
            NatsStreamDefinition stream = new NatsStreamDefinition("EVENTS", List.of("events.created"), null, null, null, null, null, null, null, Map.of());
            NatsConsumerDefinition consumer = new NatsConsumerDefinition(
                    "EVENTS", "worker", null, List.of(), "None", null, null, null, null, null, true, Map.of());

            NatsValidationReport report = operations.validate(List.of(stream), List.of(consumer)).toCompletableFuture().join();

            assertThat(report.valid()).isFalse();
            assertThat(report.errors()).anySatisfy(error -> assertThat(error).contains("ack-policy is not Explicit"));
            assertThat(report.errors()).anySatisfy(error -> assertThat(error).contains("no duplicate-window"));
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void validateAcceptsExactOnceProcessingWhenRequiredTopologyIsPresent() throws Exception {
        Connection connection = mock(Connection.class);
        JetStreamManagement management = mock(JetStreamManagement.class);
        when(connection.jetStreamManagement()).thenReturn(management);
        StreamInfo streamInfo = mock(StreamInfo.class);
        StreamConfiguration streamConfiguration = StreamConfiguration.builder()
                .name("EVENTS")
                .subjects("events.created")
                .duplicateWindow(Duration.ofMinutes(2))
                .build();
        when(streamInfo.getConfiguration()).thenReturn(streamConfiguration);
        when(management.getStreamInfo("EVENTS")).thenReturn(streamInfo);
        ConsumerInfo consumerInfo = mockConsumerInfo(ConsumerConfiguration.builder().durable("worker").deliverGroup("workers").build());
        when(management.getConsumerInfo("EVENTS", "worker")).thenReturn(consumerInfo);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            DefaultNatsTopologyOperations operations = new DefaultNatsTopologyOperations(new StaticConnectionManager(connection), executor);
            NatsStreamDefinition stream = new NatsStreamDefinition(
                    "EVENTS", List.of("events.created"), null, null, null, null, null, null, Duration.ofMinutes(2), Map.of());
            NatsConsumerDefinition consumer = new NatsConsumerDefinition(
                    "EVENTS", "worker", null, List.of(), "Explicit", null, null, null, null, "workers", true, Map.of());

            NatsValidationReport report = operations.validate(List.of(stream), List.of(consumer)).toCompletableFuture().join();

            assertThat(report.errors()).isEmpty();
            assertThat(report.valid()).isTrue();
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void validateReportsErrorsForMissingStreamAndConsumer() throws Exception {
        Connection connection = mock(Connection.class);
        JetStreamManagement management = mock(JetStreamManagement.class);
        when(connection.jetStreamManagement()).thenReturn(management);
        when(management.getStreamInfo("EVENTS")).thenThrow(new RuntimeException("missing stream"));
        when(management.getConsumerInfo("EVENTS", "worker")).thenThrow(new RuntimeException("missing consumer"));
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            DefaultNatsTopologyOperations operations = new DefaultNatsTopologyOperations(new StaticConnectionManager(connection), executor);
            NatsStreamDefinition stream = new NatsStreamDefinition("EVENTS", List.of("events.created"), null, null, null, null, null, null, null, Map.of());
            NatsConsumerDefinition consumer = new NatsConsumerDefinition("EVENTS", "worker", null, List.of(), null, null, null, null, null, Map.of());

            NatsValidationReport report = operations.validate(List.of(stream), List.of(consumer)).toCompletableFuture().join();

            assertThat(report.valid()).isFalse();
            assertThat(report.errors()).hasSize(2);
            verify(management, never()).updateStream(any());
        } finally {
            executor.shutdownNow();
        }
    }

    private ConsumerInfo mockConsumerInfo(ConsumerConfiguration configuration) {
        ConsumerInfo consumerInfo = mock(ConsumerInfo.class);
        when(consumerInfo.getConsumerConfiguration()).thenReturn(configuration);
        return consumerInfo;
    }

    private static final class StaticConnectionManager implements NatsConnectionManager {
        private final Connection natsConnection;

        private StaticConnectionManager(Connection natsConnection) {
            this.natsConnection = natsConnection;
        }

        @Override
        public java.util.concurrent.CompletionStage<Connection> connectAsync() {
            return CompletableFuture.completedFuture(natsConnection);
        }

        @Override
        public java.util.concurrent.CompletionStage<Connection> connection() {
            return CompletableFuture.completedFuture(natsConnection);
        }

        @Override
        public Optional<Connection> currentConnection() {
            return Optional.of(natsConnection);
        }

        @Override
        public Connection.Status status() {
            return Connection.Status.CONNECTED;
        }

        @Override
        public java.util.concurrent.CompletionStage<Void> drainAsync() {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public java.util.concurrent.CompletionStage<Void> closeAsync() {
            return CompletableFuture.completedFuture(null);
        }
    }
}
