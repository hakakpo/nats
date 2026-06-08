package com.bnpparibas.nats.core;

import java.util.List;
import java.util.concurrent.CompletionStage;

public interface NatsJetStreamOperations {
    CompletionStage<NatsJetStreamPublishAck> publish(NatsJetStreamPublishRequest request);

    CompletionStage<NatsSubscriptionHandle> subscribePush(NatsJetStreamPushSubscribeRequest request, NatsMessageHandler handler);

    CompletionStage<List<NatsReceivedMessage>> fetch(NatsJetStreamPullFetchRequest request);
}
