package com.bnpparibas.nats.core;

import java.util.concurrent.CompletionStage;

public interface NatsCoreOperations {
    CompletionStage<Void> publish(NatsPublishRequest request);

    CompletionStage<NatsMessage> request(NatsRequest request);

    CompletionStage<NatsSubscriptionHandle> subscribe(NatsSubscriptionRequest request, NatsMessageHandler handler);
}
