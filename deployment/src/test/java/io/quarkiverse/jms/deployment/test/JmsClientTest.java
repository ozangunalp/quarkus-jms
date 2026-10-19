package io.quarkiverse.jms.deployment.test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import jakarta.inject.Inject;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.JMSContext;
import jakarta.jms.TextMessage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.quarkiverse.jms.JmsClient;
import io.quarkiverse.jms.JmsConversionException;
import io.quarkiverse.jms.JmsDeliveryMode;
import io.quarkiverse.jms.JmsException;
import io.quarkiverse.jms.JmsQueue;
import io.quarkiverse.jms.ReceiveMessage;
import io.quarkiverse.jms.SendMessage;
import io.quarkus.test.QuarkusExtensionTest;

public class JmsClientTest {

    @RegisterExtension
    static final QuarkusExtensionTest config = new QuarkusExtensionTest()
            .overrideConfigKey("quarkus.http.test-port", "0")
            .withApplicationRoot(jar -> jar
                    .addClasses(ConnectionFactoryProducer.class, EmbeddedJmsBroker.class,
                            TestPojo.class, Order.class)
                    .addAsResource("broker.xml"))
            .setBeforeAllCustomizer(EmbeddedJmsBroker::start)
            .setAfterAllCustomizer(EmbeddedJmsBroker::stop);

    @Inject
    JmsClient jmsClient;

    @Inject
    ConnectionFactory connectionFactory;

    // ==================== bean injection ====================

    @Test
    void jmsClientIsInjectable() {
        assertThat(jmsClient).isNotNull();
        assertThat(jmsClient.queue("test")).isNotNull();
        assertThat(jmsClient.topic("test")).isNotNull();
    }

    // ==================== exception hierarchy ====================

    @Test
    void jmsExceptionHierarchyExists() {
        assertThat(JmsException.class).isAssignableFrom(io.quarkiverse.jms.JmsSendException.class);
        assertThat(JmsException.class).isAssignableFrom(io.quarkiverse.jms.JmsReceiveException.class);
        assertThat(JmsException.class).isAssignableFrom(JmsConversionException.class);
        assertThat(JmsException.class).isAssignableFrom(io.quarkiverse.jms.JmsListenerException.class);
        assertThat(RuntimeException.class).isAssignableFrom(JmsException.class);
    }

    @Test
    void sendAndReceiveTimeoutThrowsJmsReceiveException() {
        assertThatThrownBy(() -> jmsClient.queue("empty-rr-queue")
                .message("request")
                .receiveTimeout(Duration.ofMillis(500))
                .sendAndReceive(String.class))
                .isInstanceOf(io.quarkiverse.jms.JmsReceiveException.class)
                .hasMessageContaining("Timed out");
    }

    // ==================== receiver ====================

    @Test
    void multiReceiveWithSingleConsumer() {
        JmsQueue<String> queue = jmsClient.queue("receiver-test", String.class);
        for (int i = 0; i < 5; i++) {
            queue.send("msg-" + i);
        }

        List<String> received = new ArrayList<>();
        queue.receiveWith(receiver -> {
            for (int i = 0; i < 5; i++) {
                receiver.receive().ifPresent(received::add);
            }
        });

        assertThat(received).containsExactly("msg-0", "msg-1", "msg-2", "msg-3", "msg-4");
    }

    @Test
    void receiveWithReturnValue() {
        JmsQueue<String> queue = jmsClient.queue("receiver-return-test", String.class);
        queue.send("collect-me");
        queue.send("collect-me-too");

        List<String> collected = queue.receiveWith(receiver -> {
            List<String> results = new ArrayList<>();
            receiver.receive().ifPresent(results::add);
            receiver.receive().ifPresent(results::add);
            return results;
        });

        assertThat(collected).containsExactly("collect-me", "collect-me-too");
    }

    @Test
    void receiveWithSelector() {
        try (JMSContext ctx = connectionFactory.createContext()) {
            var dest = ctx.createQueue("receiver-selector-test");
            ctx.createProducer().setProperty("level", "HIGH").send(dest, "important");
            ctx.createProducer().setProperty("level", "LOW").send(dest, "trivial");
            ctx.createProducer().setProperty("level", "HIGH").send(dest, "urgent");
        }

        List<String> received = jmsClient.queue("receiver-selector-test", String.class)
                .select("level = 'HIGH'")
                .receiveWith(receiver -> {
                    List<String> results = new ArrayList<>();
                    receiver.receive().ifPresent(results::add);
                    receiver.receive().ifPresent(results::add);
                    return results;
                });

        assertThat(received).containsExactly("important", "urgent");
    }

    @Test
    void receiveWithTimeout() {
        JmsQueue<String> queue = jmsClient.queue("receiver-timeout-test", String.class);

        queue.receiveTimeout(Duration.ofMillis(200)).receiveWith(receiver -> {
            Optional<String> result = receiver.receive();
            assertThat(result).isEmpty();
        });
    }

    @Test
    void receiveWithMessage() {
        JmsQueue<String> queue = jmsClient.queue("receiver-msg-test", String.class);
        queue.send("with-headers");

        queue.receiveWith(receiver -> {
            receiver.receiveMessage().ifPresent(msg -> {
                assertThat(msg.payload()).isEqualTo("with-headers");
                assertThat(msg.message()).isNotNull();
            });
        });
    }

    @Test
    void sessionReturnedToPoolAfterCallback() {
        JmsQueue<String> queue = jmsClient.queue("receiver-pool-test", String.class);
        queue.send("before");

        queue.receiveWith(receiver -> {
            receiver.receive();
        });

        queue.send("after");
        Optional<String> result = queue.receive();
        assertThat(result).contains("after");
    }

    // ==================== send and receive ====================

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

    @Test
    void destinationReceiveMessageWithClass() {
        jmsClient.queue("dest-rm-class-queue").send("dest-rm-class");

        Optional<ReceiveMessage<String>> result = jmsClient.queue("dest-rm-class-queue")
                .receiveMessage(String.class);

        assertThat(result).isPresent();
        assertThat(result.get().payload()).isEqualTo("dest-rm-class");
    }

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

    // ==================== converter ====================

    @Test
    void stringSendAndReceive() {
        jmsClient.queue("conv-string").send("hello");

        Optional<String> result = jmsClient.queue("conv-string").receive(String.class);
        assertThat(result).contains("hello");
    }

    @Test
    void byteArraySendAndReceive() {
        byte[] data = { 1, 2, 3, 4 };
        jmsClient.queue("conv-bytes").send(data);

        Optional<byte[]> result = jmsClient.queue("conv-bytes").receive(byte[].class);
        assertThat(result).isPresent();
        assertThat(result.get()).containsExactly(1, 2, 3, 4);
    }

    @Test
    void mapSendAndReceive() {
        Map<String, Object> map = Map.of("key", "value", "num", 42);
        jmsClient.queue("conv-map").send(map);

        @SuppressWarnings("unchecked")
        Optional<Map<String, Object>> result = (Optional<Map<String, Object>>) (Optional<?>) jmsClient.queue("conv-map")
                .receive(Map.class);
        assertThat(result).isPresent();
        assertThat(result.get()).containsEntry("key", "value");
    }

    @Test
    void rawMessagePassthrough() throws Exception {
        try (JMSContext ctx = connectionFactory.createContext()) {
            var msg = ctx.createTextMessage("raw-text");
            msg.setStringProperty("custom", "prop");
            ctx.createProducer().send(ctx.createQueue("conv-raw"), msg);
        }

        Optional<ReceiveMessage<String>> result = jmsClient.queue("conv-raw", String.class).receiveMessage();
        assertThat(result).isPresent();
        assertThat(result.get().payload()).isEqualTo("raw-text");
        assertThat(result.get().message().getStringProperty("custom")).isEqualTo("prop");
    }

    @Test
    void pojoSendAndReceiveViaJackson() {
        jmsClient.queue("conv-pojo").send(new TestPojo("alice", 30));

        Optional<TestPojo> result = jmsClient.queue("conv-pojo").receive(TestPojo.class);
        assertThat(result).isPresent();
        assertThat(result.get().name).isEqualTo("alice");
        assertThat(result.get().age).isEqualTo(30);
    }

    @Test
    void objectClassReceiveReturnsStringForTextMessage() {
        jmsClient.queue("conv-object").send("text-as-object");

        Optional<Object> result = jmsClient.queue("conv-object").receive();
        assertThat(result).isPresent();
        assertThat(result.get()).isInstanceOf(String.class).isEqualTo("text-as-object");
    }

    @Test
    void untypedReceiveReturnsStringForTextMessage() {
        try (JMSContext ctx = connectionFactory.createContext()) {
            ctx.createProducer().send(ctx.createQueue("untyped-recv-queue"), "hello-untyped");
        }

        Optional<Object> result = jmsClient.queue("untyped-recv-queue").receive();
        assertThat(result).isPresent();
        assertThat(result.get()).isInstanceOf(String.class);
        assertThat(result.get()).isEqualTo("hello-untyped");
    }

    @Test
    void untypedReceiveWithExplicitClassWorks() {
        try (JMSContext ctx = connectionFactory.createContext()) {
            ctx.createProducer().send(ctx.createQueue("untyped-explicit-queue"), "explicit-type");
        }

        Optional<String> result = jmsClient.queue("untyped-explicit-queue").receive(String.class);
        assertThat(result).isPresent().contains("explicit-type");
    }

    @Test
    void receiveInvalidJsonThrowsConversionException() {
        try (JMSContext ctx = connectionFactory.createContext()) {
            ctx.createProducer().send(ctx.createQueue("conv-error-queue"),
                    ctx.createTextMessage("not valid json"));
        }

        assertThatThrownBy(() -> jmsClient.queue("conv-error-queue")
                .receiveTimeout(Duration.ofSeconds(5))
                .receive(Order.class))
                .isInstanceOf(JmsConversionException.class);
    }

    @Test
    void receiveTypeMismatchThrowsConversionException() {
        try (JMSContext ctx = connectionFactory.createContext()) {
            ctx.createProducer().send(ctx.createQueue("conv-mismatch-queue"),
                    ctx.createBytesMessage());
        }

        assertThatThrownBy(() -> jmsClient.queue("conv-mismatch-queue")
                .receiveTimeout(Duration.ofSeconds(5))
                .receive(Order.class))
                .isInstanceOf(JmsConversionException.class);
    }

    @Test
    void receiveValidJsonDeserializesSuccessfully() {
        jmsClient.queue("conv-ok-queue").send(new Order(1, "widget"));

        Optional<Order> result = jmsClient.queue("conv-ok-queue")
                .receiveTimeout(Duration.ofSeconds(5))
                .receive(Order.class);

        assertThat(result).isPresent();
        assertThat(result.get().id).isEqualTo(1);
        assertThat(result.get().product).isEqualTo("widget");
    }

    // ==================== browse messages ====================

    @Test
    void browseMessagesReturnsPayloadAndRawMessage() throws Exception {
        try (JMSContext ctx = connectionFactory.createContext()) {
            TextMessage msg = ctx.createTextMessage("browse-payload");
            msg.setStringProperty("origin", "test");
            ctx.createProducer().send(ctx.createQueue("browse-msg-queue"), msg);
        }

        List<ReceiveMessage<String>> results = jmsClient.queue("browse-msg-queue", String.class)
                .browseMessages();

        assertThat(results).hasSize(1);
        assertThat(results.get(0).payload()).isEqualTo("browse-payload");
        assertThat(results.get(0).message()).isNotNull();
        assertThat(results.get(0).message().getStringProperty("origin")).isEqualTo("test");
    }

    @Test
    void browseMessagesWithSelector() throws Exception {
        try (JMSContext ctx = connectionFactory.createContext()) {
            TextMessage high = ctx.createTextMessage("high-priority");
            high.setIntProperty("priority", 10);
            ctx.createProducer().send(ctx.createQueue("browse-sel-queue"), high);

            TextMessage low = ctx.createTextMessage("low-priority");
            low.setIntProperty("priority", 1);
            ctx.createProducer().send(ctx.createQueue("browse-sel-queue"), low);
        }

        List<ReceiveMessage<String>> highOnly = jmsClient.queue("browse-sel-queue", String.class)
                .browseMessages("priority > 5");

        assertThat(highOnly).hasSize(1);
        assertThat(highOnly.get(0).payload()).isEqualTo("high-priority");
    }

    // ==================== helpers ====================

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

    // ==================== inner types ====================

    public static class TestPojo {
        public String name;
        public int age;

        public TestPojo() {
        }

        public TestPojo(String name, int age) {
            this.name = name;
            this.age = age;
        }
    }

    public static class Order {
        public int id;
        public String product;

        public Order() {
        }

        public Order(int id, String product) {
            this.id = id;
            this.product = product;
        }
    }
}
