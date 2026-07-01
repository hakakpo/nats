package com.bnpparibas.nats.spring.autoconfigure;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.bnpparibas.nats.core.NatsConnectionManager;
import com.bnpparibas.nats.core.NatsConsumerDefinition;
import com.bnpparibas.nats.core.NatsCoreOperations;
import com.bnpparibas.nats.core.NatsJetStreamOperations;
import com.bnpparibas.nats.core.NatsTopologyOperations;
import com.bnpparibas.nats.core.NatsValidationReport;
import io.nats.client.Connection;
import io.nats.client.Options;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

class BnppNatsAutoConfigurationTest {
    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(
                    BnppNatsAutoConfiguration.class,
                    BnppNatsActuatorAutoConfiguration.class))
            .withPropertyValues("bnpp.nats.auto-start=false");

    @Test
    void createsStarterBeansWhenEnabled() {
        contextRunner.run(context -> assertThat(context)
                .hasSingleBean(NatsConnectionManager.class)
                .hasSingleBean(NatsCoreOperations.class)
                .hasSingleBean(NatsJetStreamOperations.class)
                .hasSingleBean(NatsTopologyOperations.class)
                .hasSingleBean(Options.class)
                .hasSingleBean(HealthIndicator.class));
    }

    @Test
    void backsOffWhenDisabled() {
        contextRunner
                .withPropertyValues("bnpp.nats.enabled=false")
                .run(context -> assertThat(context).doesNotHaveBean(NatsConnectionManager.class));
    }

    @Test
    void mapsPropertiesToNatsOptions() {
        contextRunner
                .withPropertyValues(
                        "bnpp.nats.servers[0]=nats://n1:4222",
                        "bnpp.nats.servers[1]=nats://n2:4222",
                        "bnpp.nats.connection-name=orders-service",
                        "bnpp.nats.connection-timeout=3s",
                        "bnpp.nats.reconnect-wait=4s",
                        "bnpp.nats.max-reconnects=12",
                        "bnpp.nats.subject-validation=strict",
                        "bnpp.nats.auth.token=secret")
                .run(context -> {
                    Options options = context.getBean(Options.class);

                    assertThat(options.getConnectionName()).isEqualTo("orders-service");
                    assertThat(options.getConnectionTimeout()).isEqualTo(Duration.ofSeconds(3));
                    assertThat(options.getReconnectWait()).isEqualTo(Duration.ofSeconds(4));
                    assertThat(options.getMaxReconnect()).isEqualTo(12);
                    assertThat(options.getToken()).isEqualTo("secret");
                    assertThat(options.getServers()).hasSize(2);
                });
    }

    @Test
    void bindsJetStreamPublishRecoveryProperties() {
        contextRunner
                .withPropertyValues("bnpp.nats.jet-stream.auto-create-stream-on-publish-failure=true")
                .run(context -> {
                    BnppNatsProperties properties = context.getBean(BnppNatsProperties.class);

                    assertThat(properties.getJetStream().isAutoCreateStreamOnPublishFailure()).isTrue();
                });
    }

    @Test
    void bindsConsumerExactOnceProcessingProperties() {
        contextRunner
                .withPropertyValues(
                        "bnpp.nats.topology.consumers[0].stream=EVENTS",
                        "bnpp.nats.topology.consumers[0].durable=worker",
                        "bnpp.nats.topology.consumers[0].deliver-group=workers",
                        "bnpp.nats.topology.consumers[0].exact-once-processing=true")
                .run(context -> {
                    BnppNatsProperties properties = context.getBean(BnppNatsProperties.class);
                    BnppNatsProperties.Consumer consumer = properties.getTopology().getConsumers().getFirst();

                    assertThat(consumer.getDeliverGroup()).isEqualTo("workers");
                    assertThat(consumer.isExactOnceProcessing()).isTrue();
                });
    }

    @Test
    void natsOptionsFactorySupportsUsernamePasswordAndNoSubjectValidation() {
        BnppNatsProperties properties = new BnppNatsProperties();
        properties.setSubjectValidation(BnppNatsProperties.SubjectValidation.NONE);
        properties.getAuth().setUsername("user");
        properties.getAuth().setPassword("pwd");

        Options options = NatsOptionsFactory.from(properties);

        assertThat(options.getUsername()).isEqualTo("user");
        assertThat(options.getPassword()).isEqualTo("pwd");
        assertThat(options.getServers()).hasSize(1);
    }

    @Test
    void lifecycleDoesNothingWhenAutoStartIsDisabled() {
        BnppNatsProperties properties = new BnppNatsProperties();
        properties.setAutoStart(false);
        NatsConnectionManager manager = mock(NatsConnectionManager.class);
        NatsTopologyOperations topology = mock(NatsTopologyOperations.class);
        NatsLifecycle lifecycle = new NatsLifecycle(properties, manager, topology);

        lifecycle.start();

        assertThat(lifecycle.isRunning()).isFalse();
        verify(manager, never()).connectAsync();
    }

    @Test
    void lifecycleCreatesAndValidatesTopologyWhenEnabled() {
        BnppNatsProperties properties = new BnppNatsProperties();
        properties.setCreateTopology(true);
        BnppNatsProperties.Stream stream = new BnppNatsProperties.Stream();
        stream.setName("EVENTS");
        stream.setSubjects(List.of("events.>"));
        properties.getTopology().setStreams(List.of(stream));
        BnppNatsProperties.Consumer consumer = new BnppNatsProperties.Consumer();
        consumer.setStream("EVENTS");
        consumer.setDurable("worker");
        properties.getTopology().setConsumers(List.of(consumer));

        NatsConnectionManager manager = mock(NatsConnectionManager.class);
        NatsTopologyOperations topology = mock(NatsTopologyOperations.class);
        when(manager.connectAsync()).thenReturn(CompletableFuture.completedFuture(mock(Connection.class)));
        when(manager.connection()).thenReturn(CompletableFuture.completedFuture(mock(Connection.class)));
        when(topology.ensureStream(org.mockito.ArgumentMatchers.any())).thenReturn(CompletableFuture.completedFuture(null));
        when(topology.ensureConsumer(org.mockito.ArgumentMatchers.any())).thenReturn(CompletableFuture.completedFuture(null));
        when(topology.validate(anyList(), anyList())).thenReturn(CompletableFuture.completedFuture(NatsValidationReport.emptyValidReport()));
        NatsLifecycle lifecycle = new NatsLifecycle(properties, manager, topology);

        lifecycle.start();

        assertThat(lifecycle.isRunning()).isTrue();
        verify(topology).ensureStream(org.mockito.ArgumentMatchers.any());
        verify(topology).ensureConsumer(org.mockito.ArgumentMatchers.any());
        verify(topology).validate(anyList(), anyList());
    }

    @Test
    void lifecycleMapsConsumerExactOnceProcessingTopology() {
        BnppNatsProperties properties = new BnppNatsProperties();
        properties.setCreateTopology(true);
        properties.setValidateTopology(false);
        BnppNatsProperties.Consumer consumer = new BnppNatsProperties.Consumer();
        consumer.setStream("EVENTS");
        consumer.setDurable("worker");
        consumer.setDeliverGroup("workers");
        consumer.setExactOnceProcessing(true);
        properties.getTopology().setConsumers(List.of(consumer));

        NatsConnectionManager manager = mock(NatsConnectionManager.class);
        NatsTopologyOperations topology = mock(NatsTopologyOperations.class);
        when(manager.connectAsync()).thenReturn(CompletableFuture.completedFuture(mock(Connection.class)));
        when(manager.connection()).thenReturn(CompletableFuture.completedFuture(mock(Connection.class)));
        when(topology.ensureConsumer(org.mockito.ArgumentMatchers.any())).thenReturn(CompletableFuture.completedFuture(null));
        NatsLifecycle lifecycle = new NatsLifecycle(properties, manager, topology);

        lifecycle.start();

        ArgumentCaptor<NatsConsumerDefinition> consumerCaptor = ArgumentCaptor.forClass(NatsConsumerDefinition.class);
        verify(topology).ensureConsumer(consumerCaptor.capture());
        assertThat(consumerCaptor.getValue().deliverGroup()).isEqualTo("workers");
        assertThat(consumerCaptor.getValue().exactOnceProcessing()).isTrue();
    }

    @Test
    void lifecycleFailsWhenValidationFailsAndFailFastEnabled() {
        BnppNatsProperties properties = new BnppNatsProperties();
        properties.setFailOnInvalidTopology(true);
        NatsConnectionManager manager = mock(NatsConnectionManager.class);
        NatsTopologyOperations topology = mock(NatsTopologyOperations.class);
        when(manager.connectAsync()).thenReturn(CompletableFuture.completedFuture(mock(Connection.class)));
        when(manager.connection()).thenReturn(CompletableFuture.completedFuture(mock(Connection.class)));
        when(topology.validate(anyList(), anyList())).thenReturn(CompletableFuture.completedFuture(
                new NatsValidationReport(false, List.of("missing"), List.of())));
        NatsLifecycle lifecycle = new NatsLifecycle(properties, manager, topology);

        assertThatThrownBy(lifecycle::start).hasRootCauseInstanceOf(IllegalStateException.class);
        assertThat(lifecycle.isRunning()).isFalse();
    }

    @Test
    void healthIndicatorReportsDownWhenNoConnectionAndUpWhenConnected() {
        NatsConnectionManager downManager = mock(NatsConnectionManager.class);
        when(downManager.currentConnection()).thenReturn(Optional.empty());
        when(downManager.status()).thenReturn(Connection.Status.DISCONNECTED);
        Health down = new BnppNatsActuatorAutoConfiguration().natsHealthIndicator(downManager).health();
        assertThat(down.getStatus().getCode()).isEqualTo("DOWN");

        NatsConnectionManager upManager = mock(NatsConnectionManager.class);
        Connection connection = mock(Connection.class);
        when(upManager.currentConnection()).thenReturn(Optional.of(connection));
        when(upManager.status()).thenReturn(Connection.Status.CONNECTED);
        when(connection.getStatus()).thenReturn(Connection.Status.CONNECTED);
        when(connection.getConnectedUrl()).thenReturn("nats://localhost:4222");
        when(connection.getServers()).thenReturn(List.of("nats://localhost:4222"));
        Health up = new BnppNatsActuatorAutoConfiguration().natsHealthIndicator(upManager).health();
        assertThat(up.getStatus().getCode()).isEqualTo("UP");
        assertThat(up.getDetails()).containsEntry("connectedUrl", "nats://localhost:4222");
    }
}
