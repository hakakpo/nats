package com.bnpparibas.nats.core;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public record NatsConsumerDefinition(
        String stream,
        String durable,
        String name,
        List<String> filterSubjects,
        String ackPolicy,
        String deliverPolicy,
        Duration ackWait,
        Long maxDeliver,
        Long maxAckPending,
        Map<String, String> metadata
) {
    public NatsConsumerDefinition {
        stream = Objects.requireNonNull(stream, "stream is required");
        durable = Objects.requireNonNull(durable, "durable is required");
        filterSubjects = filterSubjects == null ? List.of() : List.copyOf(filterSubjects);
        metadata = metadata == null ? Map.of() : Map.copyOf(metadata);
    }
}
