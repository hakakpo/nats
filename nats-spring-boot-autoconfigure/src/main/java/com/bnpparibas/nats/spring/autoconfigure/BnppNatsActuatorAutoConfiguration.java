package com.bnpparibas.nats.spring.autoconfigure;

import com.bnpparibas.nats.core.NatsConnectionManager;
import io.nats.client.Connection;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;

@AutoConfiguration(after = BnppNatsAutoConfiguration.class)
@ConditionalOnClass(HealthIndicator.class)
@ConditionalOnBean(NatsConnectionManager.class)
public class BnppNatsActuatorAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean(name = "natsHealthIndicator")
    HealthIndicator natsHealthIndicator(NatsConnectionManager connectionManager) {
        return () -> connectionManager.currentConnection()
                .map(connection -> healthFor(connectionManager, connection))
                .orElseGet(() -> Health.down().withDetail("status", connectionManager.status()).build());
    }

    private Health healthFor(NatsConnectionManager manager, Connection connection) {
        Health.Builder builder = manager.status() == Connection.Status.CONNECTED ? Health.up() : Health.down();
        return builder
                .withDetail("status", connection.getStatus())
                .withDetail("connectedUrl", connection.getConnectedUrl())
                .withDetail("servers", connection.getServers())
                .withDetail("outgoingPendingMessages", connection.outgoingPendingMessageCount())
                .withDetail("outgoingPendingBytes", connection.outgoingPendingBytes())
                .build();
    }
}
