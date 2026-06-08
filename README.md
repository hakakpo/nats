# BNPP NATS Spring Boot Starter

Shared Java 21 / Spring Boot 3.5 library for NATS Core and JetStream, built on top of the official `io.nats:jnats` client.

The goal of this project is to provide a reusable internal starter that applications can import from Artifactory and configure with standard Spring Boot properties. It exposes a non-blocking application-facing API while keeping the official NATS Java client as the protocol implementation.

## What This Library Provides

- Spring Boot starter packaging for easy adoption by other services.
- Core NATS publish, request/reply and subscriptions.
- JetStream async publishing with publish acknowledgements.
- Optional JetStream publish recovery: create a missing stream after a publish failure, then retry once.
- JetStream push subscriptions with durable consumers.
- JetStream pull fetch support.
- Explicit message acknowledgement helpers: `ack`, `ackSync`, `nak`, `nakWithDelay`, `term`, `inProgress`.
- Declarative stream and consumer topology creation.
- Startup topology validation before applications start publishing.
- Actuator `HealthIndicator` when Spring Boot Actuator is present.
- Configurable connection, reconnection, authentication and TLS options.
- Java 21 virtual-thread executor by default for blocking `jnats` operations.
- JaCoCo coverage gate enforced at build time.

## Module Layout

```text
bnpp-nats-parent
|-- nats-core
|-- nats-spring-boot-autoconfigure
`-- nats-spring-boot-starter
```

### `nats-core`

Framework-independent API and implementation around `jnats`.

Main types:

- `NatsConnectionManager`
- `NatsCoreOperations`
- `NatsJetStreamOperations`
- `NatsTopologyOperations`
- `NatsReceivedMessage`
- `NatsPublishRequest`
- `NatsJetStreamPublishRequest`
- `NatsStreamDefinition`
- `NatsConsumerDefinition`

### `nats-spring-boot-autoconfigure`

Spring Boot auto-configuration module.

It provides:

- `BnppNatsProperties` bound to `bnpp.nats.*`.
- `Options` creation for the official NATS Java client.
- Auto-configured `NatsConnectionManager`.
- Auto-configured Core, JetStream and topology operations.
- Startup lifecycle for connection and topology checks.
- Actuator health integration.

### `nats-spring-boot-starter`

Dependency-only starter imported by applications.

It intentionally contains no code. The warning `JAR will be empty` during Maven packaging is expected for this module.

## Non-Blocking Contract

The official Java NATS client uses JVM socket internals and still has synchronous operations for connection, admin calls, subscriptions and some pull-consumer operations.

This starter does not claim to make `jnats` network I/O fully non-blocking. Instead, it guarantees that application threads do not need to block on these operations:

- Public APIs return `CompletionStage`.
- Synchronous `jnats` operations are isolated on `natsOperationExecutor`.
- The default executor uses Java 21 virtual threads.
- The library has no Servlet or WebFlux dependency, so it is compatible with applications running on Tomcat or Netty.

For high-throughput durable messaging, prefer JetStream publish APIs and durable pull or push consumers.

## Installation In A Consumer Application

After publishing this project to Artifactory, add the starter dependency:

```xml
<dependency>
    <groupId>com.bnpparibas</groupId>
    <artifactId>nats-spring-boot-starter</artifactId>
    <version>0.0.1-SNAPSHOT</version>
</dependency>
```

The application can then inject the operations directly:

```java
@Service
class OrderMessagingService {
    private final NatsCoreOperations nats;
    private final NatsJetStreamOperations jetStream;

    OrderMessagingService(NatsCoreOperations nats, NatsJetStreamOperations jetStream) {
        this.nats = nats;
        this.jetStream = jetStream;
    }
}
```

## Minimal Configuration

```yaml
bnpp:
  nats:
    servers:
      - nats://localhost:4222
    connection-name: orders-service
    auto-start: true
    reconnect-on-connect: true
```

## Full Configuration Example

```yaml
bnpp:
  nats:
    enabled: true
    auto-start: true
    reconnect-on-connect: true
    create-topology: true
    validate-topology: true
    fail-on-invalid-topology: true

    servers:
      - nats://nats-1:4222
      - nats://nats-2:4222
    connection-name: orders-service
    connection-timeout: 5s
    socket-write-timeout: 30s
    ping-interval: 30s
    reconnect-wait: 2s
    max-reconnects: -1
    reconnect-buffer-size: 8388608
    no-echo: true
    use-dispatcher-with-executor: true
    subject-validation: lenient # lenient | none | strict

    executor:
      virtual-threads: true
      platform-threads: 8

    jet-stream:
      auto-create-stream-on-publish-failure: false

    auth:
      credentials-path: /etc/nats/orders.creds
      # token: token-value
      # username: user
      # password: password

    tls:
      secure: true
      trust-store-path: /etc/tls/truststore.p12
      trust-store-password: changeit
      key-store-path: /etc/tls/keystore.p12
      key-store-password: changeit
      algorithm: SunX509
      tls-first: false

    topology:
      streams:
        - name: ORDERS
          subjects:
            - orders.>
          storage-type: File
          retention-policy: Limits
          replicas: 3
          max-messages: 1000000
          max-bytes: 1073741824
          max-age: 7d
          duplicate-window: 2m
          metadata:
            owner: platform
            domain: orders
      consumers:
        - stream: ORDERS
          durable: orders-worker
          name: orders-worker
          filter-subjects:
            - orders.created
            - orders.updated
          ack-policy: Explicit
          deliver-policy: All
          ack-wait: 30s
          max-deliver: 5
          max-ack-pending: 1000
          metadata:
            owner: orders-team
```

## Property Override Model

Consumer applications override properties through normal Spring Boot configuration sources:

- `application.yml`
- `application.properties`
- environment variables
- command line arguments
- Spring Cloud Config or any externalized config source

Example environment variables:

```bash
BNPP_NATS_SERVERS_0=nats://nats-prod-1:4222
BNPP_NATS_SERVERS_1=nats://nats-prod-2:4222
BNPP_NATS_CONNECTION_NAME=orders-api
BNPP_NATS_AUTH_CREDENTIALS_PATH=/etc/nats/orders.creds
BNPP_NATS_CREATE_TOPOLOGY=false
BNPP_NATS_VALIDATE_TOPOLOGY=true
```

## Startup Topology Management

Topology is configured through `bnpp.nats.topology.*`.

Startup behavior is controlled by three flags:

```yaml
bnpp:
  nats:
    create-topology: true
    validate-topology: true
    fail-on-invalid-topology: true
```

Behavior:

- `create-topology=true`: streams are added or updated, consumers are added or updated.
- `validate-topology=true`: configured streams and consumers are checked before the application is considered ready.
- `fail-on-invalid-topology=true`: startup fails if validation reports missing or incompatible resources.

Recommended production mode depends on ownership:

- If applications own their NATS topology, use `create-topology=true` and `validate-topology=true`.
- If platform teams own topology, use `create-topology=false`, `validate-topology=true`, `fail-on-invalid-topology=true`.

## Publish-Side Safety

JetStream publish can include `expectedStream` and `messageId`.

Use `expectedStream` to prevent accidental publication to an unexpected stream. Use `messageId` to activate JetStream duplicate detection when the stream has a duplicate window configured.

```java
NatsJetStreamPublishRequest request = new NatsJetStreamPublishRequest(
        "orders.created",
        Map.of("content-type", "application/json"),
        payload,
        "ORDERS",
        "ORDERS",
        orderId,
        null,
        null,
        Duration.ofSeconds(5)
);

CompletionStage<NatsJetStreamPublishAck> ack = jetStream.publish(request);
```

The library does not perform an admin `getStreamInfo` before every publish. That would add latency and load. Instead, topology is validated once at startup, and publish expectations are used for runtime safety.

### Missing Stream Recovery On Publish

If a service must publish even when the target stream has not been provisioned yet, enable publish-side recovery:

```yaml
bnpp:
  nats:
    jet-stream:
      auto-create-stream-on-publish-failure: true
```

Behavior:

- The first publish is attempted directly. No `getStreamInfo` pre-check is executed before each publish.
- If the publish fails with a missing-stream signal, the library creates the stream through JetStream management.
- The stream name is taken from `NatsJetStreamPublishRequest.stream()` first, then from `expectedStream()`.
- The created stream is bound to the exact subject used for the failed publish.
- After stream creation, the library retries the publish once.
- A log entry is emitted when the stream is created.
- Other failures, such as authorization errors or invalid publish expectations, are not recovered.

This mode is useful for lower environments, dynamic workloads or applications that own their stream lifecycle. In controlled production environments, prefer declarative topology with `create-topology=true` and `validate-topology=true` so streams are created and validated at startup.

## Core NATS Publish

```java
CompletionStage<Void> published = nats.publish(
        NatsPublishRequest.of("orders.created", payload)
);
```

Core NATS is fast but provides no persistence and no server-side publish acknowledgement. Use it for low-latency transient messaging.

## Request / Reply

```java
CompletionStage<NatsMessage> response = nats.request(
        NatsRequest.of("orders.validate", payload, Duration.ofSeconds(2))
);
```

## Core NATS Subscription

```java
nats.subscribe(NatsSubscriptionRequest.queue("orders.created", "orders-workers"), message -> {
    byte[] payload = message.data();
    // handle the message
});
```

## JetStream Push Consumer

```java
NatsJetStreamPushSubscribeRequest subscription = NatsJetStreamPushSubscribeRequest
        .durable("orders.created", "ORDERS", "orders-worker");

jetStream.subscribePush(subscription, message -> {
    process(message.data())
            .thenCompose(ignored -> message.ack())
            .exceptionally(error -> {
                message.nak();
                return null;
            });
});
```

## JetStream Pull Fetch

```java
NatsJetStreamPullFetchRequest fetch = new NatsJetStreamPullFetchRequest(
        "orders.created",
        "ORDERS",
        "orders-worker",
        50,
        Duration.ofSeconds(1)
);

CompletionStage<List<NatsReceivedMessage>> messages = jetStream.fetch(fetch);
```

## Message Acknowledgement Rules

For JetStream consumers using explicit ack policy:

- Call `ack()` after successful processing.
- Call `nak()` when processing failed and redelivery is expected.
- Call `nakWithDelay(Duration)` to delay redelivery.
- Call `inProgress()` for long-running work to extend the ack wait window.
- Call `term()` only when the message must not be redelivered.
- Use `ackSync(Duration)` only when the application requires stronger acknowledgement confirmation; it costs more than `ack()`.

## Health Indicator

When Spring Boot Actuator is present, the starter creates a NATS health indicator.

It reports:

- connection status
- connected URL
- known servers
- outgoing pending messages
- outgoing pending bytes

Typical endpoint:

```text
GET /actuator/health
```

## Build And Test

Run all tests and coverage checks:

```bash
./mvnw verify
```

On Windows:

```powershell
.\mvnw.cmd verify
```

Current verification includes:

- `nats-core` unit tests.
- `nats-spring-boot-autoconfigure` context and lifecycle tests.
- JaCoCo report generation.
- JaCoCo minimum line coverage check at 80% per module.

Coverage reports are generated under:

```text
nats-core/target/site/jacoco/index.html
nats-spring-boot-autoconfigure/target/site/jacoco/index.html
```

The build excludes pure Spring properties classes from the coverage gate because they are configuration holders made mostly of getters and setters.

## Publishing To Artifactory

Example deployment command:

```bash
./mvnw clean deploy \
  -DaltDeploymentRepository=bnpp-artifactory::default::https://artifactory.example/repository/maven-releases
```

A consuming project should depend only on:

```xml
<dependency>
    <groupId>com.bnpparibas</groupId>
    <artifactId>nats-spring-boot-starter</artifactId>
    <version>0.0.1-SNAPSHOT</version>
</dependency>
```

## Operational Recommendations

- Prefer JetStream for business events that require durability or redelivery.
- Always set `messageId` for idempotent JetStream publication when possible.
- Use `expectedStream` for publish safety.
- Keep message handlers short; delegate long work to application executors or async services.
- Do not call blocking APIs from Netty event-loop threads; use the provided `CompletionStage` API.
- Keep `validate-topology=true` in production to fail fast on missing streams or consumers.
- Enable `jet-stream.auto-create-stream-on-publish-failure=true` only when the application is allowed to create streams dynamically.
- Set `create-topology=false` in production if topology is managed by platform automation.
- Use credentials files or NKeys rather than static username/password where possible.
- Configure TLS for non-local clusters.

## Known Limits

- This library isolates blocking calls but does not replace the internal I/O model of `jnats`.
- Core NATS subjects are not administrative resources. They cannot be validated like Kafka topics.
- JetStream exactly-once behavior requires application-level discipline: duplicate window, message IDs and explicit acknowledgements.
- No integration tests with a real NATS container are included yet. The current suite is unit and Spring auto-configuration focused.

## Next Extensions

Potential next iterations:

- Testcontainers-based NATS integration tests.
- Micrometer meters for publish latency, ack latency and subscription errors.
- Annotation-based listeners similar to `@NatsListener`.
- Optional Reactor adapter returning `Mono` and `Flux` for WebFlux applications.
- Cached subject-to-stream validation before publish when teams require stricter runtime checks.

