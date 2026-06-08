package com.bnpparibas.nats.core;

import java.util.Map;

public record NatsMessage(
        String subject,
        String replyTo,
        Map<String, String> headers,
        byte[] data,
        boolean jetStream
) {
    public NatsMessage {
        headers = headers == null ? Map.of() : Map.copyOf(headers);
        data = data == null ? new byte[0] : data.clone();
    }

    @Override
    public byte[] data() {
        return data.clone();
    }
}
