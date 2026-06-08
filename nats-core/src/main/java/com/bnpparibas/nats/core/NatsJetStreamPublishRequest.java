package com.bnpparibas.nats.core;

import java.time.Duration;
import java.util.Map;
import java.util.Objects;

public record NatsJetStreamPublishRequest(
        String subject,
        Map<String, String> headers,
        byte[] data,
        String stream,
        String expectedStream,
        String messageId,
        Long expectedLastSequence,
        String expectedLastMessageId,
        Duration streamTimeout
) {
    public NatsJetStreamPublishRequest {
        subject = Objects.requireNonNull(subject, "subject is required");
        headers = headers == null ? Map.of() : Map.copyOf(headers);
        data = data == null ? new byte[0] : data.clone();
    }

    public static NatsJetStreamPublishRequest of(String subject, byte[] data) {
        return new NatsJetStreamPublishRequest(subject, Map.of(), data, null, null, null, null, null, null);
    }

    @Override
    public byte[] data() {
        return data.clone();
    }
}
