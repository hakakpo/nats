package com.bnpparibas.nats.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.JetStream;
import io.nats.client.JetStreamManagement;
import io.nats.client.JetStreamSubscription;
import io.nats.client.PublishOptions;
import io.nats.client.PullSubscribeOptions;
import io.nats.client.PushSubscribeOptions;
import io.nats.client.api.PublishAck;
import io.nats.client.api.StreamConfiguration;
import io.nats.client.impl.Headers;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

class DefaultNatsJetStreamOperationsTest {

    @Test
    void publishReturnsMappedAck() throws Exception {
        Connection connection = mock(Connection.class);
        JetStream jetStream = mock(JetStream.class);
        PublishAck publishAck = mock(PublishAck.class);
        when(connection.jetStream()).thenReturn(jetStream);
        when(publishAck.getStream()).thenReturn("ORDERS");
        when(publishAck.getSeqno()).thenReturn(42L);
        when(publishAck.isDuplicate()).thenReturn(false);
        when(jetStream.publishAsync(eq("orders.created"), any(Headers.class), eq("body".getBytes()), any(PublishOptions.class)))
                .thenReturn(CompletableFuture.completedFuture(publishAck));

        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            DefaultNatsJetStreamOperations operations = new DefaultNatsJetStreamOperations(new StaticConnectionManager(connection), executor);
            NatsJetStreamPublishRequest request = new NatsJetStreamPublishRequest(
                    "orders.created", Map.of("id", "1"), "body".getBytes(), "ORDERS", "ORDERS", "msg-1", null, null, null);

            NatsJetStreamPublishAck ack = operations.publish(request).toCompletableFuture().join();

            assertThat(ack.stream()).isEqualTo("ORDERS");
            assertThat(ack.sequence()).isEqualTo(42L);
            assertThat(ack.duplicate()).isFalse();
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void publishCreatesMissingStreamAndRetriesOnceWhenRecoveryIsEnabled() throws Exception {
        Connection connection = mock(Connection.class);
        JetStream jetStream = mock(JetStream.class);
        JetStreamManagement management = mock(JetStreamManagement.class);
        PublishAck publishAck = mock(PublishAck.class);
        when(connection.jetStream()).thenReturn(jetStream);
        when(connection.jetStreamManagement()).thenReturn(management);
        when(publishAck.getStream()).thenReturn("ORDERS");
        when(publishAck.getSeqno()).thenReturn(43L);
        when(publishAck.isDuplicate()).thenReturn(false);
        when(jetStream.publishAsync(eq("orders.created"), any(Headers.class), eq("body".getBytes()), any(PublishOptions.class)))
                .thenReturn(
                        CompletableFuture.failedFuture(new IllegalStateException("no stream matches subject")),
                        CompletableFuture.completedFuture(publishAck));

        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            DefaultNatsJetStreamOperations operations = new DefaultNatsJetStreamOperations(new StaticConnectionManager(connection), executor, true);
            NatsJetStreamPublishRequest request = new NatsJetStreamPublishRequest(
                    "orders.created", Map.of(), "body".getBytes(), "ORDERS", "ORDERS", null, null, null, null);

            NatsJetStreamPublishAck ack = operations.publish(request).toCompletableFuture().join();

            assertThat(ack.stream()).isEqualTo("ORDERS");
            assertThat(ack.sequence()).isEqualTo(43L);
            ArgumentCaptor<StreamConfiguration> streamCaptor = ArgumentCaptor.forClass(StreamConfiguration.class);
            verify(management).addStream(streamCaptor.capture());
            assertThat(streamCaptor.getValue().getName()).isEqualTo("ORDERS");
            assertThat(streamCaptor.getValue().getSubjects()).containsExactly("orders.created");
            verify(jetStream, times(2)).publishAsync(eq("orders.created"), any(Headers.class), eq("body".getBytes()), any(PublishOptions.class));
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void publishDoesNotCreateMissingStreamWhenRecoveryIsDisabled() throws Exception {
        Connection connection = mock(Connection.class);
        JetStream jetStream = mock(JetStream.class);
        JetStreamManagement management = mock(JetStreamManagement.class);
        when(connection.jetStream()).thenReturn(jetStream);
        when(connection.jetStreamManagement()).thenReturn(management);
        when(jetStream.publishAsync(eq("orders.created"), any(Headers.class), eq("body".getBytes()), any(PublishOptions.class)))
                .thenReturn(CompletableFuture.failedFuture(new IllegalStateException("no stream matches subject")));

        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            DefaultNatsJetStreamOperations operations = new DefaultNatsJetStreamOperations(new StaticConnectionManager(connection), executor, false);
            NatsJetStreamPublishRequest request = new NatsJetStreamPublishRequest(
                    "orders.created", Map.of(), "body".getBytes(), "ORDERS", "ORDERS", null, null, null, null);

            assertThatThrownBy(() -> operations.publish(request).toCompletableFuture().join())
                    .hasRootCauseMessage("no stream matches subject");
            verify(management, never()).addStream(any(StreamConfiguration.class));
            verify(jetStream).publishAsync(eq("orders.created"), any(Headers.class), eq("body".getBytes()), any(PublishOptions.class));
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void publishDoesNotCreateStreamForUnrelatedPublishFailure() throws Exception {
        Connection connection = mock(Connection.class);
        JetStream jetStream = mock(JetStream.class);
        JetStreamManagement management = mock(JetStreamManagement.class);
        when(connection.jetStream()).thenReturn(jetStream);
        when(connection.jetStreamManagement()).thenReturn(management);
        when(jetStream.publishAsync(eq("orders.created"), any(Headers.class), eq("body".getBytes()), any(PublishOptions.class)))
                .thenReturn(CompletableFuture.failedFuture(new IllegalStateException("authorization violation")));

        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            DefaultNatsJetStreamOperations operations = new DefaultNatsJetStreamOperations(new StaticConnectionManager(connection), executor, true);
            NatsJetStreamPublishRequest request = new NatsJetStreamPublishRequest(
                    "orders.created", Map.of(), "body".getBytes(), "ORDERS", "ORDERS", null, null, null, null);

            assertThatThrownBy(() -> operations.publish(request).toCompletableFuture().join())
                    .hasRootCauseMessage("authorization violation");
            verify(management, never()).addStream(any(StreamConfiguration.class));
            verify(jetStream).publishAsync(eq("orders.created"), any(Headers.class), eq("body".getBytes()), any(PublishOptions.class));
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void subscribePushWithoutQueueCreatesDispatcherSubscriptionAndStopsIt() throws Exception {
        Connection connection = mock(Connection.class);
        JetStream jetStream = mock(JetStream.class);
        Dispatcher dispatcher = mock(Dispatcher.class);
        JetStreamSubscription subscription = mock(JetStreamSubscription.class);
        when(connection.jetStream()).thenReturn(jetStream);
        when(connection.createDispatcher()).thenReturn(dispatcher);
        when(jetStream.subscribe(eq("orders.created"), eq(dispatcher), any(io.nats.client.MessageHandler.class), eq(false), any(PushSubscribeOptions.class)))
                .thenReturn(subscription);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            DefaultNatsJetStreamOperations operations = new DefaultNatsJetStreamOperations(new StaticConnectionManager(connection), executor);
            NatsJetStreamPushSubscribeRequest request = NatsJetStreamPushSubscribeRequest.durable("orders.created", "ORDERS", "worker");

            NatsSubscriptionHandle handle = operations.subscribePush(request, message -> { }).toCompletableFuture().join();
            handle.stopAsync().toCompletableFuture().join();

            assertThat(handle.subject()).isEqualTo("orders.created");
            verify(subscription).unsubscribe();
            verify(connection).closeDispatcher(dispatcher);
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void subscribePushWithQueueUsesQueueOverload() throws Exception {
        Connection connection = mock(Connection.class);
        JetStream jetStream = mock(JetStream.class);
        Dispatcher dispatcher = mock(Dispatcher.class);
        JetStreamSubscription subscription = mock(JetStreamSubscription.class);
        when(connection.jetStream()).thenReturn(jetStream);
        when(connection.createDispatcher()).thenReturn(dispatcher);
        when(jetStream.subscribe(eq("orders.created"), eq("workers"), eq(dispatcher), any(io.nats.client.MessageHandler.class), eq(true), any(PushSubscribeOptions.class)))
                .thenReturn(subscription);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            DefaultNatsJetStreamOperations operations = new DefaultNatsJetStreamOperations(new StaticConnectionManager(connection), executor);
            NatsJetStreamPushSubscribeRequest request = new NatsJetStreamPushSubscribeRequest(
                    "orders.created", "workers", "ORDERS", "worker", "worker", List.of("orders.created"), true, false,
                    Duration.ofSeconds(30), 100L);

            NatsSubscriptionHandle handle = operations.subscribePush(request, message -> { }).toCompletableFuture().join();

            assertThat(handle.queueGroup()).isEqualTo("workers");
            verify(jetStream).subscribe(eq("orders.created"), eq("workers"), eq(dispatcher), any(io.nats.client.MessageHandler.class), eq(true), any(PushSubscribeOptions.class));
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void durableQueueFactorySetsQueueGroupOnSubscriptionConfiguration() throws Exception {
        Connection connection = mock(Connection.class);
        JetStream jetStream = mock(JetStream.class);
        Dispatcher dispatcher = mock(Dispatcher.class);
        JetStreamSubscription subscription = mock(JetStreamSubscription.class);
        when(connection.jetStream()).thenReturn(jetStream);
        when(connection.createDispatcher()).thenReturn(dispatcher);
        when(jetStream.subscribe(eq("orders.created"), eq("workers"), eq(dispatcher), any(io.nats.client.MessageHandler.class), eq(false), any(PushSubscribeOptions.class)))
                .thenReturn(subscription);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            DefaultNatsJetStreamOperations operations = new DefaultNatsJetStreamOperations(new StaticConnectionManager(connection), executor);
            NatsJetStreamPushSubscribeRequest request = NatsJetStreamPushSubscribeRequest.durableQueue(
                    "orders.created", "ORDERS", "worker", "workers");

            operations.subscribePush(request, message -> { }).toCompletableFuture().join();

            ArgumentCaptor<PushSubscribeOptions> optionsCaptor = ArgumentCaptor.forClass(PushSubscribeOptions.class);
            verify(jetStream).subscribe(eq("orders.created"), eq("workers"), eq(dispatcher), any(io.nats.client.MessageHandler.class), eq(false), optionsCaptor.capture());
            assertThat(optionsCaptor.getValue().getDeliverGroup()).isEqualTo("workers");
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void fetchPullMessagesAndUnsubscribes() throws Exception {
        Connection connection = mock(Connection.class);
        JetStream jetStream = mock(JetStream.class);
        JetStreamSubscription subscription = mock(JetStreamSubscription.class);
        io.nats.client.Message message = mock(io.nats.client.Message.class);
        when(connection.jetStream()).thenReturn(jetStream);
        when(jetStream.subscribe(eq("orders.created"), any(PullSubscribeOptions.class))).thenReturn(subscription);
        when(subscription.fetch(2, Duration.ofMillis(100))).thenReturn(List.of(message));
        when(message.getSubject()).thenReturn("orders.created");
        when(message.getData()).thenReturn("payload".getBytes());
        when(message.hasHeaders()).thenReturn(false);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            DefaultNatsJetStreamOperations operations = new DefaultNatsJetStreamOperations(new StaticConnectionManager(connection), executor);
            NatsJetStreamPullFetchRequest request = new NatsJetStreamPullFetchRequest("orders.created", "ORDERS", "worker", 2, Duration.ofMillis(100));

            List<NatsReceivedMessage> messages = operations.fetch(request).toCompletableFuture().join();

            assertThat(messages).hasSize(1);
            assertThat(messages.getFirst().subject()).isEqualTo("orders.created");
            verify(subscription).unsubscribe();
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
