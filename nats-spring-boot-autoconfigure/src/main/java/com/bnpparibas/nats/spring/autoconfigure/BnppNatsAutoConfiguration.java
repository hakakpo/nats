package com.bnpparibas.nats.spring.autoconfigure;

import com.bnpparibas.nats.core.DefaultNatsConnectionManager;
import com.bnpparibas.nats.core.DefaultNatsCoreOperations;
import com.bnpparibas.nats.core.DefaultNatsJetStreamOperations;
import com.bnpparibas.nats.core.DefaultNatsTopologyOperations;
import com.bnpparibas.nats.core.NatsConnectionManager;
import com.bnpparibas.nats.core.NatsCoreOperations;
import com.bnpparibas.nats.core.NatsJetStreamOperations;
import com.bnpparibas.nats.core.NatsTopologyOperations;
import io.nats.client.Nats;
import io.nats.client.Options;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@AutoConfiguration
@ConditionalOnClass(Nats.class)
@ConditionalOnProperty(prefix = "bnpp.nats", name = "enabled", havingValue = "true", matchIfMissing = true)
@EnableConfigurationProperties(BnppNatsProperties.class)
public class BnppNatsAutoConfiguration {

    @Bean(destroyMethod = "shutdown")
    @ConditionalOnMissingBean(name = "natsOperationExecutor")
    ExecutorService natsOperationExecutor(BnppNatsProperties properties) {
        if (properties.getExecutor().isVirtualThreads()) {
            return Executors.newVirtualThreadPerTaskExecutor();
        }
        int threads = Math.max(1, properties.getExecutor().getPlatformThreads());
        return Executors.newFixedThreadPool(threads, Thread.ofPlatform().name("bnpp-nats-", 0).factory());
    }

    @Bean
    @ConditionalOnMissingBean
    Options natsOptions(BnppNatsProperties properties) {
        return NatsOptionsFactory.from(properties);
    }

    @Bean
    @ConditionalOnMissingBean
    NatsConnectionManager natsConnectionManager(Options options, ExecutorService natsOperationExecutor, BnppNatsProperties properties) {
        return new DefaultNatsConnectionManager(options, natsOperationExecutor, properties.isReconnectOnConnect());
    }

    @Bean
    @ConditionalOnMissingBean
    NatsCoreOperations natsCoreOperations(NatsConnectionManager connectionManager, ExecutorService natsOperationExecutor) {
        return new DefaultNatsCoreOperations(connectionManager, natsOperationExecutor);
    }

    @Bean
    @ConditionalOnMissingBean
    NatsJetStreamOperations natsJetStreamOperations(
            NatsConnectionManager connectionManager,
            ExecutorService natsOperationExecutor,
            BnppNatsProperties properties) {
        return new DefaultNatsJetStreamOperations(
                connectionManager,
                natsOperationExecutor,
                properties.getJetStream().isAutoCreateStreamOnPublishFailure());
    }

    @Bean
    @ConditionalOnMissingBean
    NatsTopologyOperations natsTopologyOperations(NatsConnectionManager connectionManager, ExecutorService natsOperationExecutor) {
        return new DefaultNatsTopologyOperations(connectionManager, natsOperationExecutor);
    }

    @Bean
    @ConditionalOnMissingBean
    NatsLifecycle natsLifecycle(
            BnppNatsProperties properties,
            NatsConnectionManager connectionManager,
            NatsTopologyOperations topologyOperations) {
        return new NatsLifecycle(properties, connectionManager, topologyOperations);
    }
}
