package com.bnpparibas.nats.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.nats.client.Message;
import io.nats.client.impl.Headers;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

class NatsMessageMappingTest {

    @Test
    void mapsHeadersAndMessageFields() {
        Message message = mock(Message.class);
        Headers headers = new Headers().put("content-type", "application/json").put("trace", "abc");
        when(message.hasHeaders()).thenReturn(true);
        when(message.getHeaders()).thenReturn(headers);
        when(message.getSubject()).thenReturn("orders.created");
        when(message.getReplyTo()).thenReturn("_INBOX.1");
        when(message.getData()).thenReturn("{}".getBytes());
        when(message.isJetStream()).thenReturn(true);

        NatsMessage mapped = NatsHeaderMapper.toNatsMessage(message);

        assertThat(mapped.subject()).isEqualTo("orders.created");
        assertThat(mapped.replyTo()).isEqualTo("_INBOX.1");
        assertThat(mapped.headers()).containsEntry("content-type", "application/json");
        assertThat(mapped.jetStream()).isTrue();
        assertThat(new String(mapped.data())).isEqualTo("{}");
    }

    @Test
    void receivedMessageDelegatesAckOperationsAsynchronously() {
        Message message = mock(Message.class);
        when(message.getSubject()).thenReturn("orders.created");
        when(message.getReplyTo()).thenReturn(null);
        when(message.getData()).thenReturn("payload".getBytes());
        when(message.hasHeaders()).thenReturn(false);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            NatsReceivedMessage received = new NatsReceivedMessage(message, executor);

            received.ack().toCompletableFuture().join();
            received.nak().toCompletableFuture().join();
            received.nakWithDelay(Duration.ofMillis(5)).toCompletableFuture().join();
            received.term().toCompletableFuture().join();
            received.inProgress().toCompletableFuture().join();

            assertThat(received.subject()).isEqualTo("orders.created");
            assertThat(new String(received.data())).isEqualTo("payload");
            assertThat(received.headers()).isEqualTo(Map.of());
            assertThat(received.unwrap()).isSameAs(message);
            verify(message).ack();
            verify(message).nak();
            verify(message).nakWithDelay(Duration.ofMillis(5));
            verify(message).term();
            verify(message).inProgress();
        } finally {
            executor.shutdownNow();
        }
    }
}
