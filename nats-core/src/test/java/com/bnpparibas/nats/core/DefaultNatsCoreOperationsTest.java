package com.bnpparibas.nats.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.Message;
import io.nats.client.Subscription;
import io.nats.client.impl.Headers;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

class DefaultNatsCoreOperationsTest {

    @Test
    void publishDelegatesToConnectionWithoutBlockingCallerContract() {
        Connection connection = mock(Connection.class);
        NatsConnectionManager manager = new StaticConnectionManager(connection);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            DefaultNatsCoreOperations operations = new DefaultNatsCoreOperations(manager, executor);

            operations.publish(new NatsPublishRequest("orders.created", null, Map.of("event", "order"), "ok".getBytes()))
                    .toCompletableFuture()
                    .join();

            verify(connection).publish(eq("orders.created"), any(Headers.class), eq("ok".getBytes()));
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void publishWithReplyToDelegatesToConnectionReplyOverload() {
        Connection connection = mock(Connection.class);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            DefaultNatsCoreOperations operations = new DefaultNatsCoreOperations(new StaticConnectionManager(connection), executor);

            operations.publish(new NatsPublishRequest("orders.request", "reply.subject", Map.of(), "payload".getBytes()))
                    .toCompletableFuture()
                    .join();

            verify(connection).publish(eq("orders.request"), eq("reply.subject"), any(Headers.class), eq("payload".getBytes()));
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void requestMapsAsyncResponse() {
        Connection connection = mock(Connection.class);
        Message response = mock(Message.class);
        when(response.getSubject()).thenReturn("orders.response");
        when(response.getReplyTo()).thenReturn(null);
        when(response.getData()).thenReturn("accepted".getBytes());
        when(response.hasHeaders()).thenReturn(false);
        when(connection.requestWithTimeout(eq("orders.validate"), any(Headers.class), eq("payload".getBytes()), eq(Duration.ofSeconds(1))))
                .thenReturn(CompletableFuture.completedFuture(response));
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            DefaultNatsCoreOperations operations = new DefaultNatsCoreOperations(new StaticConnectionManager(connection), executor);

            NatsMessage mapped = operations.request(NatsRequest.of("orders.validate", "payload".getBytes(), Duration.ofSeconds(1)))
                    .toCompletableFuture()
                    .join();

            assertThat(mapped.subject()).isEqualTo("orders.response");
            assertThat(new String(mapped.data())).isEqualTo("accepted");
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void subscribeCreatesDispatcherAndHandleCanStopIt() {
        Connection connection = mock(Connection.class);
        Dispatcher dispatcher = mock(Dispatcher.class);
        Subscription subscription = mock(Subscription.class);
        when(connection.createDispatcher()).thenReturn(dispatcher);
        when(dispatcher.subscribe(eq("orders.created"), any(io.nats.client.MessageHandler.class))).thenReturn(subscription);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            DefaultNatsCoreOperations operations = new DefaultNatsCoreOperations(new StaticConnectionManager(connection), executor);

            NatsSubscriptionHandle handle = operations.subscribe(NatsSubscriptionRequest.subject("orders.created"), message -> { })
                    .toCompletableFuture()
                    .join();
            handle.stopAsync().toCompletableFuture().join();

            assertThat(handle.subject()).isEqualTo("orders.created");
            assertThat(handle.queueGroup()).isNull();
            verify(connection).closeDispatcher(dispatcher);
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void subscribeWithQueueUsesQueueGroup() {
        Connection connection = mock(Connection.class);
        Dispatcher dispatcher = mock(Dispatcher.class);
        Subscription subscription = mock(Subscription.class);
        when(connection.createDispatcher()).thenReturn(dispatcher);
        when(dispatcher.subscribe(eq("orders.created"), eq("workers"), any(io.nats.client.MessageHandler.class))).thenReturn(subscription);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            DefaultNatsCoreOperations operations = new DefaultNatsCoreOperations(new StaticConnectionManager(connection), executor);

            NatsSubscriptionHandle handle = operations.subscribe(NatsSubscriptionRequest.queue("orders.created", "workers"), message -> { })
                    .toCompletableFuture()
                    .join();

            assertThat(handle.queueGroup()).isEqualTo("workers");
            verify(dispatcher).subscribe(eq("orders.created"), eq("workers"), any(io.nats.client.MessageHandler.class));
        } finally {
            executor.shutdownNow();
        }
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
