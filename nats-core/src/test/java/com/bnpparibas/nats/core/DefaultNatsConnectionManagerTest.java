package com.bnpparibas.nats.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.nats.client.Connection;
import io.nats.client.Options;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

class DefaultNatsConnectionManagerTest {

    @Test
    void reportsDisconnectedBeforeConnectionStartsAndCloseIsIdempotent() {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Options options = Options.builder()
                    .server("nats://localhost:4222")
                    .connectionTimeout(Duration.ofMillis(50))
                    .build();
            DefaultNatsConnectionManager manager = new DefaultNatsConnectionManager(options, executor, false);

            assertThat(manager.currentConnection()).isEmpty();
            assertThat(manager.status()).isEqualTo(Connection.Status.DISCONNECTED);

            manager.drainAsync().toCompletableFuture().join();
            manager.closeAsync().toCompletableFuture().join();
            manager.close();

            assertThat(manager.currentConnection()).isEmpty();
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void usesExistingConnectionFutureForStatusDrainAndClose() throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Options options = Options.builder()
                    .server("nats://localhost:4222")
                    .connectionTimeout(Duration.ofMillis(50))
                    .build();
            DefaultNatsConnectionManager manager = new DefaultNatsConnectionManager(options, executor, false);
            Connection connection = mock(Connection.class);
            when(connection.getStatus()).thenReturn(Connection.Status.CONNECTED);
            CompletableFuture<Boolean> drained = CompletableFuture.completedFuture(Boolean.TRUE);
            when(connection.drain(options.getConnectionTimeout())).thenReturn(drained);
            injectConnection(manager, CompletableFuture.completedFuture(connection));

            assertThat(manager.connectAsync().toCompletableFuture().join()).isSameAs(connection);
            assertThat(manager.connection().toCompletableFuture().join()).isSameAs(connection);
            assertThat(manager.currentConnection()).contains(connection);
            assertThat(manager.status()).isEqualTo(Connection.Status.CONNECTED);

            manager.drainAsync().toCompletableFuture().join();
            manager.closeAsync().toCompletableFuture().join();

            verify(connection).drain(options.getConnectionTimeout());
            verify(connection).close();
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void defaultCloseMethodsJoinAsyncOperations() {
        RecordingSubscriptionHandle handle = new RecordingSubscriptionHandle();
        handle.close();
        assertThat(handle.closed).isTrue();

        RecordingConnectionManager manager = new RecordingConnectionManager();
        manager.close();
        assertThat(manager.closed).isTrue();

        NatsClientException simple = new NatsClientException("simple");
        NatsClientException withCause = new NatsClientException("cause", new IllegalStateException("bad"));
        assertThat(simple).hasMessage("simple");
        assertThat(withCause).hasRootCauseInstanceOf(IllegalStateException.class);
    }

    @SuppressWarnings("unchecked")
    private void injectConnection(DefaultNatsConnectionManager manager, CompletableFuture<Connection> future) throws Exception {
        Field field = DefaultNatsConnectionManager.class.getDeclaredField("connection");
        field.setAccessible(true);
        AtomicReference<CompletableFuture<Connection>> reference = (AtomicReference<CompletableFuture<Connection>>) field.get(manager);
        reference.set(future);
    }

    private static final class RecordingSubscriptionHandle implements NatsSubscriptionHandle {
        private boolean closed;

        @Override
        public String subject() {
            return "subject";
        }

        @Override
        public String queueGroup() {
            return null;
        }

        @Override
        public java.util.concurrent.CompletionStage<Void> stopAsync() {
            closed = true;
            return CompletableFuture.completedFuture(null);
        }
    }

    private static final class RecordingConnectionManager implements NatsConnectionManager {
        private boolean closed;

        @Override
        public java.util.concurrent.CompletionStage<Connection> connectAsync() {
            return CompletableFuture.completedFuture(mock(Connection.class));
        }

        @Override
        public java.util.concurrent.CompletionStage<Connection> connection() {
            return connectAsync();
        }

        @Override
        public java.util.Optional<Connection> currentConnection() {
            return java.util.Optional.empty();
        }

        @Override
        public Connection.Status status() {
            return Connection.Status.DISCONNECTED;
        }

        @Override
        public java.util.concurrent.CompletionStage<Void> drainAsync() {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public java.util.concurrent.CompletionStage<Void> closeAsync() {
            closed = true;
            return CompletableFuture.completedFuture(null);
        }
    }
}
