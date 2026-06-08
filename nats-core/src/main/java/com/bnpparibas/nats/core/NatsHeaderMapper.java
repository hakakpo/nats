package com.bnpparibas.nats.core;

import io.nats.client.impl.Headers;
import io.nats.client.Message;

import java.util.LinkedHashMap;
import java.util.Map;

final class NatsHeaderMapper {
    private NatsHeaderMapper() {
    }

    static Headers toNatsHeaders(Map<String, String> headers) {
        Headers natsHeaders = new Headers();
        if (headers != null) {
            headers.forEach((key, value) -> {
                if (value != null) {
                    natsHeaders.put(key, value);
                }
            });
        }
        return natsHeaders;
    }

    static Map<String, String> fromMessage(Message message) {
        if (!message.hasHeaders() || message.getHeaders() == null || message.getHeaders().isEmpty()) {
            return Map.of();
        }
        Map<String, String> mapped = new LinkedHashMap<>();
        message.getHeaders().forEach((key, values) -> {
            if (values != null && !values.isEmpty()) {
                mapped.put(key, values.getFirst());
            }
        });
        return Map.copyOf(mapped);
    }

    static NatsMessage toNatsMessage(Message message) {
        return new NatsMessage(
                message.getSubject(),
                message.getReplyTo(),
                fromMessage(message),
                message.getData(),
                message.isJetStream()
        );
    }
}
