package com.bnpparibas.nats.core;

public record NatsJetStreamPublishAck(
        String stream,
        long sequence,
        boolean duplicate,
        String domain
) {
}
