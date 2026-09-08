package io.quarkiverse.spring.jms.deployment.test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.JMSContext;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.springframework.jms.annotation.JmsListener;
import org.springframework.jms.core.JmsClient;
import org.springframework.messaging.Message;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.messaging.support.MessageBuilder;

import io.quarkus.test.QuarkusExtensionTest;

public class SpringJmsListenerTest {

    @RegisterExtension
    static final QuarkusExtensionTest config = new QuarkusExtensionTest()
            .withApplicationRoot(jar -> jar
                    .addClasses(SpringListener.class, ConnectionFactoryProducer.class,
                            EmbeddedJmsBroker.class)
                    .addAsResource("broker.xml"))
            .setBeforeAllCustomizer(EmbeddedJmsBroker::start)
            .setAfterAllCustomizer(EmbeddedJmsBroker::stop);

    @Inject
    ConnectionFactory connectionFactory;

    @Inject
    SpringListener listener;

    @Inject
    JmsClient springClient;

    @Test
    void springListenerReceivesMessages() {
        try (JMSContext ctx = connectionFactory.createContext()) {
            ctx.createProducer().send(ctx.createQueue("spring-queue"), "spring-hello");
        }

        await().atMost(10, TimeUnit.SECONDS)
                .until(() -> listener.getMessages().contains("spring-hello"));
    }

    @Test
    void springListenerWithHeaders() throws Exception {
        try (JMSContext ctx = connectionFactory.createContext()) {
            var msg = ctx.createTextMessage("header-test");
            msg.setIntProperty("priority", 7);
            ctx.createProducer().send(ctx.createQueue("spring-header-queue"), msg);
        }

        await().atMost(10, TimeUnit.SECONDS)
                .until(() -> !listener.getHeaderResults().isEmpty());
        assertThat(listener.getHeaderResults()).contains("header-test:priority=7");
    }

    @Test
    void springListenerSendTo() {
        try (JMSContext ctx = connectionFactory.createContext()) {
            ctx.createProducer().send(ctx.createQueue("spring-request-queue"), "request");
        }

        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            try (JMSContext ctx = connectionFactory.createContext()) {
                String reply = ctx.createConsumer(ctx.createQueue("spring-reply-queue"))
                        .receiveBody(String.class, 5000);
                assertThat(reply).isEqualTo("reply:request");
            }
        });
    }

    @Test
    void springJmsClientSendsMessages() {
        springClient.destination("spring-queue").send("from-spring-client");

        await().atMost(10, TimeUnit.SECONDS)
                .until(() -> listener.getMessages().contains("from-spring-client"));
    }

    @Test
    void springJmsClientSendWithHeaders() {
        springClient.destination("spring-header-queue")
                .send("header-from-client", java.util.Map.of("priority", 9));

        await().atMost(10, TimeUnit.SECONDS)
                .until(() -> listener.getHeaderResults().contains("header-from-client:priority=9"));
    }

    @Test
    void springJmsClientReceive() {
        try (JMSContext ctx = connectionFactory.createContext()) {
            ctx.createProducer().send(ctx.createQueue("spring-receive-queue"), "to-receive");
        }

        java.util.Optional<String> result = springClient.destination("spring-receive-queue")
                .withReceiveTimeout(5000)
                .receive(String.class);
        assertThat(result).isPresent().contains("to-receive");
    }

    @Test
    void springJmsClientSendMessage() {
        Message<String> msg = MessageBuilder
                .withPayload("message-api-test")
                .setHeader("category", "test")
                .build();

        springClient.destination("spring-queue").send(msg);

        await().atMost(10, TimeUnit.SECONDS)
                .until(() -> listener.getMessages().contains("message-api-test"));
    }

    @Test
    void springJmsClientReceiveMessage() {
        try (JMSContext ctx = connectionFactory.createContext()) {
            var jmsMsg = ctx.createTextMessage("raw-msg");
            jmsMsg.setStringProperty("source", "test");
            ctx.createProducer().send(ctx.createQueue("spring-message-queue"), jmsMsg);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        Optional<org.springframework.messaging.Message<?>> result = springClient
                .destination("spring-message-queue")
                .withReceiveTimeout(5000)
                .receive();

        assertThat(result).isPresent();
        assertThat(result.get().getPayload()).isEqualTo("raw-msg");
        assertThat(result.get().getHeaders()).containsKey("source");
    }

    @Test
    void messageBuilderCreatesMessage() {
        var msg = MessageBuilder
                .withPayload("builder-test")
                .setHeader("key1", "value1")
                .setHeader("key2", 42)
                .build();

        assertThat(msg.getPayload()).isEqualTo("builder-test");
        assertThat(msg.getHeaders().get("key1")).isEqualTo("value1");
        assertThat(msg.getHeaders().get("key2")).isEqualTo(42);
        assertThat(msg.getHeaders().getId()).isNotNull();
        assertThat(msg.getHeaders().getTimestamp()).isPositive();
    }

    @Test
    void messageHeadersAreImmutable() {
        var headers = new org.springframework.messaging.MessageHeaders(java.util.Map.of("key", "value"));

        assertThat(headers.get("key")).isEqualTo("value");
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> headers.put("new", "val"))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void springJmsClientWithQos() {
        springClient.destination("spring-queue")
                .withPriority(8)
                .withTimeToLive(60000)
                .send("qos-msg");

        await().atMost(10, TimeUnit.SECONDS)
                .until(() -> listener.getMessages().contains("qos-msg"));
    }

    @Test
    void springListenerWithAllHeaders() throws Exception {
        try (JMSContext ctx = connectionFactory.createContext()) {
            var msg = ctx.createTextMessage("all-headers-test");
            msg.setStringProperty("source", "unit-test");
            msg.setIntProperty("count", 42);
            ctx.createProducer().send(ctx.createQueue("spring-all-headers-queue"), msg);
        }

        await().atMost(10, TimeUnit.SECONDS)
                .until(() -> !listener.getAllHeaderResults().isEmpty());
        assertThat(listener.getAllHeaderResults().get(0)).contains("source=unit-test");
        assertThat(listener.getAllHeaderResults().get(0)).contains("count=42");
    }

    @ApplicationScoped
    public static class SpringListener {

        private final CopyOnWriteArrayList<String> messages = new CopyOnWriteArrayList<>();
        private final CopyOnWriteArrayList<String> headerResults = new CopyOnWriteArrayList<>();
        private final CopyOnWriteArrayList<String> allHeaderResults = new CopyOnWriteArrayList<>();

        @JmsListener(destination = "spring-queue")
        public void onMessage(String message) {
            messages.add(message);
        }

        @JmsListener(destination = "spring-header-queue")
        public void onHeaderMessage(String body, @Header("priority") int priority) {
            headerResults.add(body + ":priority=" + priority);
        }

        @JmsListener(destination = "spring-all-headers-queue")
        public void onAllHeadersMessage(String body,
                @org.springframework.messaging.handler.annotation.Headers java.util.Map<String, Object> headers) {
            allHeaderResults.add(body + ":" + headers.toString());
        }

        @JmsListener(destination = "spring-request-queue")
        @SendTo("spring-reply-queue")
        public String onRequest(String message) {
            return "reply:" + message;
        }

        public List<String> getMessages() {
            return messages;
        }

        public List<String> getHeaderResults() {
            return headerResults;
        }

        public List<String> getAllHeaderResults() {
            return allHeaderResults;
        }
    }
}
