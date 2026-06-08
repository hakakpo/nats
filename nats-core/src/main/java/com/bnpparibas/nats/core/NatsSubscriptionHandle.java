package com.bnpparibas.nats.core;

import java.util.concurrent.CompletionStage;

public interface NatsSubscriptionHandle extends AutoCloseable {
    String subject();

    String queueGroup();

    CompletionStage<Void> stopAsync();

    @Override
    default void close() {
        stopAsync().toCompletableFuture().join();
    }
}
