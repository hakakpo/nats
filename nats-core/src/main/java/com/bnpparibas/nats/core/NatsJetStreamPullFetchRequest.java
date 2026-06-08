package com.bnpparibas.nats.core;

import java.time.Duration;
import java.util.Objects;

public record NatsJetStreamPullFetchRequest(
        String subject,
        String stream,
        String durable,
        int batchSize,
        Duration expiresIn
) {
    public NatsJetStreamPullFetchRequest {
        subject = Objects.requireNonNull(subject, "subject is required");
        stream = Objects.requireNonNull(stream, "stream is required");
        durable = Objects.requireNonNull(durable, "durable is required");
        if (batchSize <= 0) {
            throw new IllegalArgumentException("batchSize must be greater than zero");
        }
        expiresIn = expiresIn == null ? Duration.ofSeconds(1) : expiresIn;
    }
}
