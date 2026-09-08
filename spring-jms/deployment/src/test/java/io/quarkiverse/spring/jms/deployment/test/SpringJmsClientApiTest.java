package io.quarkiverse.spring.jms.deployment.test;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import java.util.Optional;

import jakarta.inject.Inject;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.JMSContext;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.springframework.jms.core.JmsClient;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.GenericMessage;

import io.quarkus.test.QuarkusExtensionTest;

public class SpringJmsClientApiTest {

    @RegisterExtension
    static final QuarkusExtensionTest config = new QuarkusExtensionTest()
            .withApplicationRoot(jar -> jar
                    .addClasses(ConnectionFactoryProducer.class, EmbeddedJmsBroker.class)
                    .addAsResource("broker.xml"))
            .setBeforeAllCustomizer(EmbeddedJmsBroker::start)
            .setAfterAllCustomizer(EmbeddedJmsBroker::stop);

    @Inject
    JmsClient springClient;

    @Inject
    ConnectionFactory connectionFactory;

    // --- destination(Destination) ---

    @Test
    void destinationWithJmsDestinationObject() {
        try (JMSContext ctx = connectionFactory.createContext()) {
            springClient.destination(ctx.createQueue("dest-obj-queue")).send("via-dest-obj");
        }

        Optional<String> result = springClient.destination("dest-obj-queue")
                .withReceiveTimeout(5000)
                .receive(String.class);
        assertThat(result).contains("via-dest-obj");
    }

    // --- receive with selector ---

    @Test
    void receiveWithSelector() throws Exception {
        try (JMSContext ctx = connectionFactory.createContext()) {
            var low = ctx.createTextMessage("low-msg");
            low.setStringProperty("level", "low");
            ctx.createProducer().send(ctx.createQueue("spring-sel-queue"), low);

            var high = ctx.createTextMessage("high-msg");
            high.setStringProperty("level", "high");
            ctx.createProducer().send(ctx.createQueue("spring-sel-queue"), high);
        }

        Optional<String> result = springClient.destination("spring-sel-queue")
                .withReceiveTimeout(5000)
                .receive("level = 'high'", String.class);

        assertThat(result).contains("high-msg");
    }

    @Test
    void receiveRawWithSelector() throws Exception {
        try (JMSContext ctx = connectionFactory.createContext()) {
            var msg = ctx.createTextMessage("sel-raw");
            msg.setStringProperty("kind", "test");
            ctx.createProducer().send(ctx.createQueue("spring-sel-raw-queue"), msg);
        }

        Optional<Message<?>> result = springClient.destination("spring-sel-raw-queue")
                .withReceiveTimeout(5000)
                .receive("kind = 'test'");

        assertThat(result).isPresent();
    }

    // --- sendAndReceive ---

    @Test
    void sendAndReceiveWithPayload() {
        setupEchoReplier("spring-sar-queue");

        Optional<String> reply = springClient.destination("spring-sar-queue")
                .withReceiveTimeout(5000)
                .sendAndReceive("request-payload", String.class);

        assertThat(reply).contains("reply:request-payload");
    }

    @Test
    void sendAndReceiveWithHeaders() {
        setupEchoReplier("spring-sar-hdr-queue");

        Optional<String> reply = springClient.destination("spring-sar-hdr-queue")
                .withReceiveTimeout(5000)
                .sendAndReceive("hdr-request",
                        Map.of("customHeader", "value"),
                        String.class);

        assertThat(reply).contains("reply:hdr-request");
    }

    @Test
    void sendAndReceiveWithMessage() {
        setupEchoReplier("spring-sar-msg-queue");

        Message<?> request = new GenericMessage<>("msg-request",
                Map.of("requestId", "123"));

        Optional<Message<?>> reply = springClient.destination("spring-sar-msg-queue")
                .withReceiveTimeout(5000)
                .sendAndReceive(request);

        assertThat(reply).isPresent();
    }

    @Test
    void sendAndReceiveReturnsEmptyOnTimeout() {
        Optional<String> reply = springClient.destination("spring-sar-empty-queue")
                .withReceiveTimeout(500)
                .sendAndReceive("no-replier", String.class);

        assertThat(reply).isEmpty();
    }

    // --- Producer config ---

    @Test
    void withDeliveryPersistent() throws Exception {
        springClient.destination("spring-persist-queue")
                .withDeliveryPersistent(false)
                .send("non-persistent");

        try (JMSContext ctx = connectionFactory.createContext()) {
            var msg = ctx.createConsumer(ctx.createQueue("spring-persist-queue")).receive(5000);
            assertThat(msg).isNotNull();
            assertThat(msg.getJMSDeliveryMode()).isEqualTo(jakarta.jms.DeliveryMode.NON_PERSISTENT);
        }
    }

    private void setupEchoReplier(String queueName) {
        new Thread(() -> {
            try (JMSContext ctx = connectionFactory.createContext()) {
                var consumer = ctx.createConsumer(ctx.createQueue(queueName));
                var request = consumer.receive(10000);
                if (request != null && request.getJMSReplyTo() != null) {
                    String body = request.getBody(String.class);
                    ctx.createProducer().send(request.getJMSReplyTo(),
                            ctx.createTextMessage("reply:" + body));
                }
            } catch (Exception e) {
                // ignore
            }
        }).start();
        try {
            Thread.sleep(200);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
