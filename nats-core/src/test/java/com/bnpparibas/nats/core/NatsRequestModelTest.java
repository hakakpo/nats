package com.bnpparibas.nats.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

class NatsRequestModelTest {

    @Test
    void publishRequestDefensivelyCopiesHeadersAndPayload() {
        Map<String, String> headers = new HashMap<>();
        headers.put("type", "created");
        byte[] data = "payload".getBytes();

        NatsPublishRequest request = new NatsPublishRequest("events.created", null, headers, data);
        headers.put("type", "updated");
        data[0] = 'P';

        assertThat(request.headers()).containsEntry("type", "created");
        assertThat(new String(request.data())).isEqualTo("payload");
        assertThat(NatsPublishRequest.of("events.created", "data".getBytes()).subject()).isEqualTo("events.created");
    }

    @Test
    void requestUsesDefaultsAndDefensiveCopies() {
        byte[] data = "payload".getBytes();
        NatsRequest request = new NatsRequest("rpc.validate", null, data, null);
        data[0] = 'P';

        assertThat(request.headers()).isEmpty();
        assertThat(request.timeout()).isEqualTo(Duration.ofSeconds(5));
        assertThat(new String(request.data())).isEqualTo("payload");
        assertThat(NatsRequest.of("rpc.validate", "x".getBytes(), Duration.ofMillis(10)).timeout())
                .isEqualTo(Duration.ofMillis(10));
    }

    @Test
    void messageModelsExposeEmptyCollectionsAndPayloads() {
        NatsMessage message = new NatsMessage("a.b", null, null, null, false);
        NatsJetStreamPublishRequest publish = NatsJetStreamPublishRequest.of("events.created", null);
        NatsJetStreamPushSubscribeRequest push = NatsJetStreamPushSubscribeRequest.durable("events.created", "EVENTS", "worker");
        NatsValidationReport report = NatsValidationReport.emptyValidReport();

        assertThat(message.headers()).isEmpty();
        assertThat(message.data()).isEmpty();
        assertThat(publish.headers()).isEmpty();
        assertThat(publish.data()).isEmpty();
        assertThat(push.stream()).isEqualTo("EVENTS");
        assertThat(push.durable()).isEqualTo("worker");
        assertThat(report.valid()).isTrue();
    }

    @Test
    void subscriptionFactoriesAndFetchValidationWork() {
        assertThat(NatsSubscriptionRequest.subject("events.*").queueGroup()).isNull();
        assertThat(NatsSubscriptionRequest.queue("events.*", "workers").queueGroup()).isEqualTo("workers");
        assertThatThrownBy(() -> new NatsJetStreamPullFetchRequest("s", "STREAM", "durable", 0, null))
                .isInstanceOf(IllegalArgumentException.class);
        NatsJetStreamPullFetchRequest fetch = new NatsJetStreamPullFetchRequest("s", "STREAM", "durable", 10, null);
        assertThat(fetch.expiresIn()).isEqualTo(Duration.ofSeconds(1));
    }

    @Test
    void topologyDefinitionsCopyMutableInputs() {
        Map<String, String> metadata = new HashMap<>();
        metadata.put("owner", "team-a");
        NatsStreamDefinition stream = new NatsStreamDefinition("EVENTS", java.util.List.of("events.>"), "File", "Limits", 1, null, null, null, null, metadata);
        NatsConsumerDefinition consumer = new NatsConsumerDefinition("EVENTS", "worker", null, java.util.List.of("events.created"), "Explicit", "All", null, null, null, metadata);
        metadata.put("owner", "team-b");

        assertThat(stream.metadata()).containsEntry("owner", "team-a");
        assertThat(consumer.metadata()).containsEntry("owner", "team-a");
    }
}
