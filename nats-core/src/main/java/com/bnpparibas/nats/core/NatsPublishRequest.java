package com.bnpparibas.nats.core;

import java.util.Map;
import java.util.Objects;

public record NatsPublishRequest(
        String subject,
        String replyTo,
        Map<String, String> headers,
        byte[] data
) {
    public NatsPublishRequest {
        subject = Objects.requireNonNull(subject, "subject is required");
        headers = headers == null ? Map.of() : Map.copyOf(headers);
        data = data == null ? new byte[0] : data.clone();
    }

    public static NatsPublishRequest of(String subject, byte[] data) {
        return new NatsPublishRequest(subject, null, Map.of(), data);
    }

    @Override
    public byte[] data() {
        return data.clone();
    }
}
