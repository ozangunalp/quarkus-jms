# Quarkus JMS

[![Version](https://img.shields.io/maven-central/v/io.quarkiverse.jms/quarkus-jms?logo=apache-maven&style=flat-square)](https://central.sonatype.com/artifact/io.quarkiverse.jms/quarkus-jms-parent)

A Quarkus extension for JMS messaging with three complementary APIs:

- **`JmsQueue<T>` / `JmsTopic<T>`** — injectable, typed destinations for sending and receiving messages
- **`@JmsListener`** — annotation-driven message consumption with concurrency, scaling, and virtual thread support
- **Reactive Messaging connector** (`smallrye-jms`) — `@Incoming` / `@Outgoing` channel-based messaging

All three support local and XA distributed transactions, connection pooling, and health checks.

## Modules

| Module | Artifact | Description |
|--------|----------|-------------|
| Core | `quarkus-jms` | JmsClient, JmsQueue/JmsTopic, @JmsListener, session pooling, observability (Micrometer + OpenTelemetry) |
| Spring JMS | `quarkus-spring-jms` | Spring JMS API compatibility (@JmsListener, JmsTemplate, @SendTo) |
| Reactive Messaging | `quarkus-messaging-jms` | SmallRye Reactive Messaging JMS connector with XA support |

## Getting Started

Add the extension to your project:

```xml
<dependency>
    <groupId>io.quarkiverse.jms</groupId>
    <artifactId>quarkus-jms</artifactId>
</dependency>
```

You also need a JMS provider. For example, [Quarkus Artemis JMS](https://docs.quarkiverse.io/quarkus-artemis/dev/index.html) provides a ConnectionFactory bean and Dev Services:

```xml
<dependency>
    <groupId>io.quarkiverse.artemis</groupId>
    <artifactId>quarkus-artemis-jms</artifactId>
</dependency>
```

### Sending messages

```java
@Inject @Identifier("orders")
JmsQueue<Order> orders;

orders.send(order);

orders.message(order)
    .priority(5)
    .timeToLive(Duration.ofMinutes(30))
    .send();
```

### Receiving messages

```java
Optional<Order> order = orders.receive();

// Request-reply
Reply reply = orders.sendAndReceive(request, Reply.class);
```

### Annotation-driven listeners

```java
@ApplicationScoped
public class OrderProcessor {

    @JmsListener(value = "orders", concurrency = 2, maxConcurrency = 8)
    public void process(Order order) {
        // automatic deserialization, connection management, error handling
    }
}
```

### Reactive Messaging

```java
@Incoming("orders")
@Outgoing("notifications")
public String process(String order) {
    return "processed:" + order;
}
```

## Documentation

The documentation for this extension is maintained in the `docs/` directory and published at <https://docs.quarkiverse.io/quarkus-jms/dev/index.html>.

## License

This project is licensed under the [Apache License, Version 2.0](LICENSE).
