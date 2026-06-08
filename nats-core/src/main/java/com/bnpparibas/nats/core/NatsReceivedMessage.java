package com.bnpparibas.nats.core;

import io.nats.client.Message;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;

public final class NatsReceivedMessage {
    private final Message delegate;
    private final Executor executor;

    NatsReceivedMessage(Message delegate, Executor executor) {
        this.delegate = delegate;
        this.executor = executor;
    }

    public String subject() {
        return delegate.getSubject();
    }

    public String replyTo() {
        return delegate.getReplyTo();
    }

    public Map<String, String> headers() {
        return NatsHeaderMapper.fromMessage(delegate);
    }

    public byte[] data() {
        byte[] data = delegate.getData();
        return data == null ? new byte[0] : data.clone();
    }

    public boolean jetStream() {
        return delegate.isJetStream();
    }

    public CompletionStage<Void> ack() {
        return CompletableFuture.runAsync(delegate::ack, executor);
    }

    public CompletionStage<Void> ackSync(Duration timeout) {
        return CompletableFuture.runAsync(() -> {
            try {
                delegate.ackSync(timeout);
            } catch (Exception ex) {
                throw new NatsClientException("JetStream ackSync failed", ex);
            }
        }, executor);
    }

    public CompletionStage<Void> nak() {
        return CompletableFuture.runAsync(delegate::nak, executor);
    }

    public CompletionStage<Void> nakWithDelay(Duration delay) {
        return CompletableFuture.runAsync(() -> delegate.nakWithDelay(delay), executor);
    }

    public CompletionStage<Void> term() {
        return CompletableFuture.runAsync(delegate::term, executor);
    }

    public CompletionStage<Void> inProgress() {
        return CompletableFuture.runAsync(delegate::inProgress, executor);
    }

    public Message unwrap() {
        return delegate;
    }
}
