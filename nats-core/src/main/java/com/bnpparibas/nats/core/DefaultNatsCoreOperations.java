package com.bnpparibas.nats.core;

import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.MessageHandler;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;

public class DefaultNatsCoreOperations implements NatsCoreOperations {
    private final NatsConnectionManager connectionManager;
    private final ExecutorService executor;

    public DefaultNatsCoreOperations(NatsConnectionManager connectionManager, ExecutorService executor) {
        this.connectionManager = connectionManager;
        this.executor = executor;
    }

    @Override
    public CompletionStage<Void> publish(NatsPublishRequest request) {
        return connectionManager.connection().thenAcceptAsync(connection -> {
            if (request.replyTo() == null || request.replyTo().isBlank()) {
                connection.publish(request.subject(), NatsHeaderMapper.toNatsHeaders(request.headers()), request.data());
            } else {
                connection.publish(request.subject(), request.replyTo(), NatsHeaderMapper.toNatsHeaders(request.headers()), request.data());
            }
        }, executor);
    }

    @Override
    public CompletionStage<NatsMessage> request(NatsRequest request) {
        return connectionManager.connection()
                .thenCompose(connection -> connection.requestWithTimeout(
                        request.subject(),
                        NatsHeaderMapper.toNatsHeaders(request.headers()),
                        request.data(),
                        request.timeout()))
                .thenApply(NatsHeaderMapper::toNatsMessage);
    }

    @Override
    public CompletionStage<NatsSubscriptionHandle> subscribe(NatsSubscriptionRequest request, NatsMessageHandler handler) {
        return connectionManager.connection().thenApplyAsync(connection -> {
            io.nats.client.MessageHandler natsHandler = msg -> handler.onMessage(new NatsReceivedMessage(msg, executor));
            Dispatcher dispatcher = connection.createDispatcher();
            if (request.queueGroup() == null || request.queueGroup().isBlank()) {
                dispatcher.subscribe(request.subject(), natsHandler);
            } else {
                dispatcher.subscribe(request.subject(), request.queueGroup(), natsHandler);
            }
            return new DispatcherSubscriptionHandle(connection, dispatcher, request.subject(), request.queueGroup(), executor);
        }, executor);
    }

    private record DispatcherSubscriptionHandle(
            Connection connection,
            Dispatcher dispatcher,
            String subject,
            String queueGroup,
            ExecutorService executor
    ) implements NatsSubscriptionHandle {
        @Override
        public CompletionStage<Void> stopAsync() {
            return CompletableFuture.runAsync(() -> connection.closeDispatcher(dispatcher), executor);
        }
    }
}
