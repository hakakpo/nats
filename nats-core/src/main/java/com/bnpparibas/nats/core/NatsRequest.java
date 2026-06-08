package com.bnpparibas.nats.core;

import java.time.Duration;
import java.util.Map;
import java.util.Objects;

public record NatsRequest(
        String subject,
        Map<String, String> headers,
        byte[] data,
        Duration timeout
) {
    public NatsRequest {
        subject = Objects.requireNonNull(subject, "subject is required");
        headers = headers == null ? Map.of() : Map.copyOf(headers);
        data = data == null ? new byte[0] : data.clone();
        timeout = timeout == null ? Duration.ofSeconds(5) : timeout;
    }

    public static NatsRequest of(String subject, byte[] data, Duration timeout) {
        return new NatsRequest(subject, Map.of(), data, timeout);
    }

    @Override
    public byte[] data() {
        return data.clone();
    }
}
