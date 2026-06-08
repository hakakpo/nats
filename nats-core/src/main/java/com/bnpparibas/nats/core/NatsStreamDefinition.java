package com.bnpparibas.nats.core;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public record NatsStreamDefinition(
        String name,
        List<String> subjects,
        String storageType,
        String retentionPolicy,
        Integer replicas,
        Long maxMessages,
        Long maxBytes,
        Duration maxAge,
        Duration duplicateWindow,
        Map<String, String> metadata
) {
    public NatsStreamDefinition {
        name = Objects.requireNonNull(name, "stream name is required");
        subjects = subjects == null ? List.of() : List.copyOf(subjects);
        metadata = metadata == null ? Map.of() : Map.copyOf(metadata);
    }
}
