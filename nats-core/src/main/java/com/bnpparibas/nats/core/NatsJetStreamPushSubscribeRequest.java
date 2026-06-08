package com.bnpparibas.nats.core;

import java.time.Duration;
import java.util.List;
import java.util.Objects;

public record NatsJetStreamPushSubscribeRequest(
        String subject,
        String queueGroup,
        String stream,
        String durable,
        String name,
        List<String> filterSubjects,
        boolean autoAck,
        boolean ordered,
        Duration ackWait,
        Long maxAckPending
) {
    public NatsJetStreamPushSubscribeRequest {
        subject = Objects.requireNonNull(subject, "subject is required");
        filterSubjects = filterSubjects == null ? List.of() : List.copyOf(filterSubjects);
    }

    public static NatsJetStreamPushSubscribeRequest durable(String subject, String stream, String durable) {
        return new NatsJetStreamPushSubscribeRequest(subject, null, stream, durable, durable, List.of(), false, false, null, null);
    }
}
