package com.bnpparibas.nats.core;

@FunctionalInterface
public interface NatsMessageHandler {
    void onMessage(NatsReceivedMessage message);
}
