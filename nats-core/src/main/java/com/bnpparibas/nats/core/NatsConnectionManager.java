package com.bnpparibas.nats.core;

import io.nats.client.Connection;

import java.util.Optional;
import java.util.concurrent.CompletionStage;

public interface NatsConnectionManager extends AutoCloseable {
    CompletionStage<Connection> connectAsync();

    CompletionStage<Connection> connection();

    Optional<Connection> currentConnection();

    Connection.Status status();

    CompletionStage<Void> drainAsync();

    CompletionStage<Void> closeAsync();

    @Override
    default void close() {
        closeAsync().toCompletableFuture().join();
    }
}
