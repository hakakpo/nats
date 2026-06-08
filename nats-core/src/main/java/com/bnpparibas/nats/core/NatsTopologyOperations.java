package com.bnpparibas.nats.core;

import java.util.List;
import java.util.concurrent.CompletionStage;

public interface NatsTopologyOperations {
    CompletionStage<Void> ensureStream(NatsStreamDefinition stream);

    CompletionStage<Void> ensureConsumer(NatsConsumerDefinition consumer);

    CompletionStage<NatsValidationReport> validate(List<NatsStreamDefinition> streams, List<NatsConsumerDefinition> consumers);
}
