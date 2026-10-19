package io.quarkiverse.jms.deployment.test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.JMSContext;
import jakarta.jms.JMSException;
import jakarta.jms.Message;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.quarkiverse.jms.JmsClient;
import io.quarkiverse.jms.JmsListener;
import io.quarkiverse.jms.JmsQueue;
import io.quarkiverse.jms.ReceiveMessage;
import io.quarkiverse.jms.SendMessage;
import io.quarkus.test.QuarkusExtensionTest;

public class JmsClientFeaturesTest {

    @RegisterExtension
    static final QuarkusExtensionTest config = new QuarkusExtensionTest()
            .overrideConfigKey("quarkus.http.test-port", "0")
            .withApplicationRoot(jar -> jar
                    .addClasses(PropertyReceiver.class, CachingReceiver.class,
                            ConnectionFactoryProducer.class, EmbeddedJmsBroker.class)
                    .addAsResource("broker.xml"))
            .setBeforeAllCustomizer(EmbeddedJmsBroker::start)
            .setAfterAllCustomizer(EmbeddedJmsBroker::stop);

    @Inject
    JmsClient jmsClient;

    @Inject
    ConnectionFactory connectionFactory;

    @Inject
    PropertyReceiver propertyReceiver;

    @Inject
    CachingReceiver cachingReceiver;

    // --- execute ---

    @Test
    void executeGivesRawContextAccess() {
        try (JMSContext context = connectionFactory.createContext()) {
            context.createProducer().send(context.createQueue("exec-queue"), "exec-msg-1");
            context.createProducer().send(context.createQueue("exec-queue"), "exec-msg-2");
        }

        int count = jmsClient.queue("exec-queue").execute(ctx -> {
            int received = 0;
            var consumer = ctx.createConsumer(ctx.createQueue("exec-queue"));
            while (consumer.receive(500) != null) {
                received++;
            }
            return received;
        });

        assertThat(count).isEqualTo(2);
    }

    @Test
    void executeBatchSend() {
        jmsClient.queue("batch-queue").execute(ctx -> {
            var producer = ctx.createProducer();
            var queue = ctx.createQueue("batch-queue");
            for (int i = 0; i < 5; i++) {
                producer.send(queue, ctx.createTextMessage("batch-" + i));
            }
            return null;
        });

        int count = jmsClient.queue("batch-queue").execute(ctx -> {
            int received = 0;
            var consumer = ctx.createConsumer(ctx.createQueue("batch-queue"));
            while (consumer.receive(500) != null) {
                received++;
            }
            return received;
        });

        assertThat(count).isEqualTo(5);
    }

    // --- send with properties ---

    @Test
    void clientSendsWithProperties() throws JMSException {
        jmsClient.queue("props-queue")
                .message("props-msg")
                .property("customKey", "customValue")
                .correlationId("corr-123")
                .priority(7)
                .send();

        await().atMost(10, TimeUnit.SECONDS)
                .until(() -> !propertyReceiver.getMessages().isEmpty());

        Message msg = propertyReceiver.getMessages().get(0);
        assertThat(msg.getStringProperty("customKey")).isEqualTo("customValue");
        assertThat(msg.getJMSCorrelationID()).isEqualTo("corr-123");
        assertThat(msg.getJMSPriority()).isEqualTo(7);
    }

    // --- reset connection ---

    @Test
    void operationsWorkAfterReset() {
        JmsQueue<String> queue = jmsClient.queue("reset-queue", String.class);

        queue.send("before-reset");
        Optional<String> r1 = queue.receive();
        assertThat(r1).contains("before-reset");

        jmsClient.resetConnection();

        queue.send("after-reset");
        Optional<String> r2 = queue.receive();
        assertThat(r2).contains("after-reset");
    }

    @Test
    void multipleResetsDoNotThrow() {
        jmsClient.resetConnection();
        jmsClient.resetConnection();
        jmsClient.resetConnection();

        jmsClient.queue("multi-reset-queue").send("still-works");
        Optional<String> received = jmsClient.queue("multi-reset-queue", String.class).receive();
        assertThat(received).contains("still-works");
    }

    // --- connection caching ---

    @Test
    void multipleOperationsReuseConnection() {
        for (int i = 0; i < 10; i++) {
            jmsClient.queue("cache-queue").send("msg-" + i);
        }

        await().atMost(10, TimeUnit.SECONDS)
                .until(() -> cachingReceiver.getMessages().size() >= 10);

        assertThat(cachingReceiver.getMessages()).hasSize(10);
    }

    @Test
    void sendAndReceiveWithCaching() {
        jmsClient.queue("cache-sr-queue").send("cached-msg");

        Optional<String> received = jmsClient.queue("cache-sr-queue").receive(String.class);
        assertThat(received).contains("cached-msg");
    }

    @Test
    void concurrentOperationsWithCaching() throws InterruptedException {
        Thread[] threads = new Thread[5];
        for (int i = 0; i < threads.length; i++) {
            int idx = i;
            threads[i] = new Thread(() -> {
                jmsClient.queue("cache-concurrent-queue").send("thread-" + idx);
            });
            threads[i].start();
        }
        for (Thread t : threads) {
            t.join(10000);
        }

        await().atMost(10, TimeUnit.SECONDS)
                .until(() -> cachingReceiver.getConcurrentMessages().size() >= 5);

        assertThat(cachingReceiver.getConcurrentMessages()).hasSize(5);
    }

    @Test
    void consumerCachedAcrossReceives() {
        jmsClient.queue("consumer-cache-queue").send("first");
        jmsClient.queue("consumer-cache-queue").send("second");

        Optional<String> r1 = jmsClient.queue("consumer-cache-queue").receive(String.class);
        Optional<String> r2 = jmsClient.queue("consumer-cache-queue").receive(String.class);

        assertThat(r1).isPresent();
        assertThat(r2).isPresent();
        assertThat(List.of(r1.get(), r2.get())).containsExactlyInAnyOrder("first", "second");
    }

    @Test
    void cachedContextStartStopAreNoOps() {
        try (JMSContext ctx = connectionFactory.createContext()) {
            ctx.start();
            ctx.stop();
            ctx.start();
            ctx.createProducer().send(ctx.createQueue("noop-queue"), "test");
        }

        Optional<String> received = jmsClient.queue("noop-queue").receive(String.class);
        assertThat(received).contains("test");
    }

    @Test
    void multipleReceivesWithSelectorReuseConsumer() {
        try (JMSContext ctx = connectionFactory.createContext()) {
            var msg1 = ctx.createTextMessage("vip-1");
            msg1.setStringProperty("tier", "vip");
            ctx.createProducer().send(ctx.createQueue("selector-cache-queue"), msg1);

            var msg2 = ctx.createTextMessage("regular-1");
            msg2.setStringProperty("tier", "regular");
            ctx.createProducer().send(ctx.createQueue("selector-cache-queue"), msg2);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        Optional<String> vip = jmsClient.queue("selector-cache-queue")
                .select("tier = 'vip'").receive(String.class);
        assertThat(vip).contains("vip-1");
    }

    @Test
    void rawMessageReceiveWorks() {
        jmsClient.queue("raw-receive-queue").send("raw-test");

        var raw = jmsClient.queue("raw-receive-queue")
                .receiveTimeout(Duration.ofSeconds(5)).receiveMessage();
        assertThat(raw).isPresent();
        assertThat(raw.get().message()).isNotNull();
    }

    // --- API enhancements ---

    @Test
    void receiveWithTimeoutConvenience() {
        jmsClient.queue("timeout-queue").send("timeout-msg");

        Optional<String> result = jmsClient.queue("timeout-queue", String.class).receive();
        assertThat(result).isPresent().contains("timeout-msg");
    }

    @Test
    void receiveWithClassAndTimeoutConvenience() {
        jmsClient.queue("timeout-class-queue").send("typed-timeout");

        Optional<String> result = jmsClient.queue("timeout-class-queue").receive(String.class);
        assertThat(result).isPresent().contains("typed-timeout");
    }

    @Test
    void receiveMessageWithTimeoutConvenience() {
        jmsClient.queue("timeout-raw-queue").send("raw-timeout");

        Optional<ReceiveMessage<String>> result = jmsClient.queue("timeout-raw-queue", String.class)
                .receiveMessage();
        assertThat(result).isPresent();
        assertThat(result.get().payload()).isEqualTo("raw-timeout");
        assertThat(result.get().message()).isNotNull();
    }

    @Test
    void browseReturnsAllMessages() {
        var queue = jmsClient.queue("browse-limit-queue", String.class);
        queue.send("b1");
        queue.send("b2");
        queue.send("b3");

        List<String> all = queue.browse();
        assertThat(all).hasSize(3);
    }

    @Test
    void browseWithSelector() throws Exception {
        try (JMSContext ctx = connectionFactory.createContext()) {
            var msg1 = ctx.createTextMessage("vip-item");
            msg1.setStringProperty("tier", "vip");
            ctx.createProducer().send(ctx.createQueue("browse-selector-queue"), msg1);

            var msg2 = ctx.createTextMessage("regular-item");
            msg2.setStringProperty("tier", "regular");
            ctx.createProducer().send(ctx.createQueue("browse-selector-queue"), msg2);
        }

        List<String> vipOnly = jmsClient.queue("browse-selector-queue", String.class)
                .browse("tier = 'vip'");
        assertThat(vipOnly).hasSize(1).contains("vip-item");
    }

    @Test
    void replyToWithTemporaryQueue() {
        jmsClient.queue("replyto-fn-queue", String.class)
                .message(SendMessage.of("with-temp-reply"))
                .replyTo(ctx -> ctx.createTemporaryQueue()).send();

        Optional<String> received = jmsClient.queue("replyto-fn-queue", String.class).receive();
        assertThat(received).isPresent().contains("with-temp-reply");
    }

    @Test
    void replyToWithTopicViaFunction() {
        jmsClient.queue("replyto-topic-queue", String.class)
                .message(SendMessage.of("with-topic-reply"))
                .replyTo(ctx -> ctx.createTopic("reply-topic")).send();

        Optional<String> received = jmsClient.queue("replyto-topic-queue", String.class).receive();
        assertThat(received).isPresent().contains("with-topic-reply");
    }

    // --- inner beans ---

    @ApplicationScoped
    public static class PropertyReceiver {

        private final CopyOnWriteArrayList<Message> messages = new CopyOnWriteArrayList<>();

        @JmsListener("props-queue")
        public void onMessage(Message message) {
            messages.add(message);
        }

        public List<Message> getMessages() {
            return messages;
        }
    }

    @ApplicationScoped
    public static class CachingReceiver {

        private final List<String> messages = new CopyOnWriteArrayList<>();
        private final List<String> concurrentMessages = new CopyOnWriteArrayList<>();

        @JmsListener("cache-queue")
        public void onMessage(String message) {
            messages.add(message);
        }

        @JmsListener("cache-concurrent-queue")
        public void onConcurrentMessage(String message) {
            concurrentMessages.add(message);
        }

        public List<String> getMessages() {
            return messages;
        }

        public List<String> getConcurrentMessages() {
            return concurrentMessages;
        }
    }
}
