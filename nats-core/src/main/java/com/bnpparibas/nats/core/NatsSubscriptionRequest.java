package com.bnpparibas.nats.core;

import java.util.Objects;

public record NatsSubscriptionRequest(
        String subject,
        String queueGroup
) {
    public NatsSubscriptionRequest {
        subject = Objects.requireNonNull(subject, "subject is required");
    }

    public static NatsSubscriptionRequest subject(String subject) {
        return new NatsSubscriptionRequest(subject, null);
    }

    public static NatsSubscriptionRequest queue(String subject, String queueGroup) {
        return new NatsSubscriptionRequest(subject, Objects.requireNonNull(queueGroup, "queueGroup is required"));
    }
}
