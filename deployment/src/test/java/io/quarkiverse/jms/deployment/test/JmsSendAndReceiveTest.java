package io.quarkiverse.jms.deployment.test;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.Optional;

import jakarta.inject.Inject;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.JMSContext;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.quarkiverse.jms.JmsClient;
import io.quarkiverse.jms.JmsDeliveryMode;
import io.quarkiverse.jms.ReceiveMessage;
import io.quarkiverse.jms.SendMessage;
import io.quarkus.test.QuarkusExtensionTest;

public class JmsSendAndReceiveTest {

    @RegisterExtension
    static final QuarkusExtensionTest config = new QuarkusExtensionTest()
            .withApplicationRoot(jar -> jar
                    .addClasses(ConnectionFactoryProducer.class, EmbeddedJmsBroker.class)
                    .addAsResource("broker.xml"))
            .setBeforeAllCustomizer(EmbeddedJmsBroker::start)
            .setAfterAllCustomizer(EmbeddedJmsBroker::stop);

    @Inject
    JmsClient jmsClient;

    @Inject
    ConnectionFactory connectionFactory;

    // --- JmsDestination convenience sendAndReceive ---

    @Test
    void sendAndReceiveConvenience() {
        setupEchoReplier("sar-conv-queue");

        String reply = jmsClient.queue("sar-conv-queue")
                .sendAndReceive("hello", String.class);

        assertThat(reply).isEqualTo("reply:hello");
    }

    @Test
    void sendAndReceiveWithSendMessage() {
        setupEchoReplier("sar-sm-queue");

        String reply = jmsClient.queue("sar-sm-queue")
                .sendAndReceive(SendMessage.of("with-modifier"), String.class);

        assertThat(reply).isEqualTo("reply:with-modifier");
    }

    @Test
    void sendAndReceiveMessageConvenience() {
        setupEchoReplier("sar-msg-queue");

        ReceiveMessage<String> reply = jmsClient.queue("sar-msg-queue")
                .sendAndReceiveMessage("hello-rm", String.class);

        assertThat(reply.payload()).isEqualTo("reply:hello-rm");
        assertThat(reply.message()).isNotNull();
    }

    @Test
    void sendAndReceiveMessageWithSendMessage() {
        setupEchoReplier("sar-sm-msg-queue");

        ReceiveMessage<String> reply = jmsClient.queue("sar-sm-msg-queue")
                .sendAndReceiveMessage(SendMessage.of("sm-rm"), String.class);

        assertThat(reply.payload()).isEqualTo("reply:sm-rm");
    }

    // --- JmsSendBuilder sendAndReceiveMessage terminal ---

    @Test
    void builderSendAndReceiveMessage() {
        setupEchoReplier("sar-builder-msg-queue");

        ReceiveMessage<String> reply = jmsClient.queue("sar-builder-msg-queue")
                .message("builder-rm")
                .receiveTimeout(Duration.ofSeconds(5))
                .sendAndReceiveMessage(String.class);

        assertThat(reply.payload()).isEqualTo("reply:builder-rm");
        assertThat(reply.message()).isNotNull();
    }

    // --- JmsSendAndReceiveBuilder narrowing ---

    @Test
    void narrowedBuilderWithSelectAndSendAndReceive() {
        setupEchoReplier("sar-narrow-queue");

        String reply = jmsClient.queue("sar-narrow-queue")
                .message("narrow-test")
                .receiveTimeout(Duration.ofSeconds(5))
                .sendAndReceive(String.class);

        assertThat(reply).isEqualTo("reply:narrow-test");
    }

    @Test
    void narrowedBuilderChainConfigAfterNarrowing() {
        setupEchoReplier("sar-chain-queue");

        String reply = jmsClient.queue("sar-chain-queue")
                .message("chain-test")
                .receiveTimeout(Duration.ofSeconds(5))
                .priority(5)
                .correlationId("chain-corr")
                .sendAndReceive(String.class);

        assertThat(reply).isEqualTo("reply:chain-test");
    }

    // --- SendMessage modifier applied on send ---

    @Test
    void sendMessageModifierApplied() throws Exception {
        jmsClient.queue("modifier-queue")
                .send(SendMessage.of("modified",
                        msg -> msg.setStringProperty("custom", "applied")));

        try (JMSContext ctx = connectionFactory.createContext()) {
            var msg = ctx.createConsumer(ctx.createQueue("modifier-queue")).receive(5000);
            assertThat(msg).isNotNull();
            assertThat(msg.getStringProperty("custom")).isEqualTo("applied");
            assertThat(msg.getBody(String.class)).isEqualTo("modified");
        }
    }

    @Test
    void sendMessageModifierViaBuilder() throws Exception {
        jmsClient.queue("modifier-builder-queue")
                .message(SendMessage.of("builder-mod",
                        msg -> msg.setStringProperty("source", "builder")))
                .priority(7)
                .send();

        try (JMSContext ctx = connectionFactory.createContext()) {
            var msg = ctx.createConsumer(ctx.createQueue("modifier-builder-queue")).receive(5000);
            assertThat(msg).isNotNull();
            assertThat(msg.getStringProperty("source")).isEqualTo("builder");
            assertThat(msg.getJMSPriority()).isEqualTo(7);
        }
    }

    // --- Builder producer config: deliveryMode, timeToLive, deliveryDelay ---

    @Test
    void builderDeliveryMode() throws Exception {
        jmsClient.queue("dm-queue")
                .message("dm-test")
                .deliveryMode(JmsDeliveryMode.NON_PERSISTENT)
                .send();

        try (JMSContext ctx = connectionFactory.createContext()) {
            var msg = ctx.createConsumer(ctx.createQueue("dm-queue")).receive(5000);
            assertThat(msg).isNotNull();
            assertThat(msg.getJMSDeliveryMode()).isEqualTo(jakarta.jms.DeliveryMode.NON_PERSISTENT);
        }
    }

    @Test
    void builderTimeToLive() throws Exception {
        jmsClient.queue("ttl-queue")
                .message("ttl-test")
                .timeToLive(Duration.ofMinutes(5))
                .send();

        try (JMSContext ctx = connectionFactory.createContext()) {
            var msg = ctx.createConsumer(ctx.createQueue("ttl-queue")).receive(5000);
            assertThat(msg).isNotNull();
            assertThat(msg.getJMSExpiration()).isGreaterThan(0);
        }
    }

    @Test
    void builderReplyToString() throws Exception {
        jmsClient.queue("replyto-str-queue")
                .message("replyto-test")
                .replyTo("my-reply-queue")
                .send();

        try (JMSContext ctx = connectionFactory.createContext()) {
            var msg = ctx.createConsumer(ctx.createQueue("replyto-str-queue")).receive(5000);
            assertThat(msg).isNotNull();
            assertThat(msg.getJMSReplyTo()).isNotNull();
        }
    }

    // --- JmsReceiveBuilder: receiveMessage(Class), chaining select+receiveTimeout ---

    @Test
    void receiveBuilderReceiveMessageWithClass() {
        jmsClient.queue("rm-class-queue").send("rm-class-msg");

        Optional<ReceiveMessage<String>> result = jmsClient.queue("rm-class-queue")
                .receiveTimeout(Duration.ofSeconds(5))
                .receiveMessage(String.class);

        assertThat(result).isPresent();
        assertThat(result.get().payload()).isEqualTo("rm-class-msg");
    }

    @Test
    void receiveBuilderSelectAndReceiveTimeout() throws Exception {
        try (JMSContext ctx = connectionFactory.createContext()) {
            var msg = ctx.createTextMessage("chained-msg");
            msg.setStringProperty("level", "high");
            ctx.createProducer().send(ctx.createQueue("chain-recv-queue"), msg);
        }

        Optional<String> result = jmsClient.queue("chain-recv-queue")
                .select("level = 'high'")
                .receiveTimeout(Duration.ofSeconds(5))
                .receive(String.class);

        assertThat(result).isPresent().contains("chained-msg");
    }

    // --- JmsDestination receiveMessage(Class) ---

    @Test
    void destinationReceiveMessageWithClass() {
        jmsClient.queue("dest-rm-class-queue").send("dest-rm-class");

        Optional<ReceiveMessage<String>> result = jmsClient.queue("dest-rm-class-queue")
                .receiveMessage(String.class);

        assertThat(result).isPresent();
        assertThat(result.get().payload()).isEqualTo("dest-rm-class");
    }

    // --- Correlation ID ---

    @Test
    void correlationIdGeneratorAppliedOnSend() throws Exception {
        jmsClient.queue("corr-gen-queue")
                .message("gen-test")
                .correlationId(msg -> "generated-123")
                .send();

        try (JMSContext ctx = connectionFactory.createContext()) {
            var msg = ctx.createConsumer(ctx.createQueue("corr-gen-queue")).receive(5000);
            assertThat(msg).isNotNull();
            assertThat(msg.getJMSCorrelationID()).isEqualTo("generated-123");
        }
    }

    @Test
    void sharedReplyQueueAutoCorrelation() throws Exception {
        setupCorrelatingReplier("sar-shared-queue", "shared-reply-queue");

        ReceiveMessage<String> reply = jmsClient.queue("sar-shared-queue")
                .message("shared-test")
                .replyTo("shared-reply-queue")
                .receiveTimeout(Duration.ofSeconds(5))
                .sendAndReceiveMessage(String.class);

        assertThat(reply.payload()).isEqualTo("reply:shared-test");
        assertThat(reply.message().getJMSCorrelationID()).isNotNull();
    }

    @Test
    void sharedReplyQueueWithExplicitCorrelationId() {
        setupCorrelatingReplier("sar-explicit-queue", "explicit-reply-queue");

        String reply = jmsClient.queue("sar-explicit-queue")
                .message("explicit-test")
                .replyTo("explicit-reply-queue")
                .correlationId("my-corr-id")
                .receiveTimeout(Duration.ofSeconds(5))
                .sendAndReceive(String.class);

        assertThat(reply).isEqualTo("reply:explicit-test");
    }

    @Test
    void sharedReplyQueueWithCorrelationIdGenerator() {
        setupCorrelatingReplier("sar-gen-queue", "gen-reply-queue");

        String reply = jmsClient.queue("sar-gen-queue")
                .message("gen-sar-test")
                .replyTo("gen-reply-queue")
                .correlationId(msg -> "custom-gen-id")
                .receiveTimeout(Duration.ofSeconds(5))
                .sendAndReceive(String.class);

        assertThat(reply).isEqualTo("reply:gen-sar-test");
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

    private void setupCorrelatingReplier(String requestQueue, String replyQueue) {
        new Thread(() -> {
            try (JMSContext ctx = connectionFactory.createContext()) {
                var consumer = ctx.createConsumer(ctx.createQueue(requestQueue));
                var request = consumer.receive(10000);
                if (request != null) {
                    var reply = ctx.createTextMessage("reply:" + request.getBody(String.class));
                    String corrId = request.getJMSCorrelationID();
                    if (corrId == null) {
                        corrId = request.getJMSMessageID();
                    }
                    reply.setJMSCorrelationID(corrId);
                    ctx.createProducer().send(ctx.createQueue(replyQueue), reply);
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
