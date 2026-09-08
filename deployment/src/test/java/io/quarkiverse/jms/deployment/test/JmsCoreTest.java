package io.quarkiverse.jms.deployment.test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.spi.BeanManager;
import jakarta.inject.Inject;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.JMSContext;
import jakarta.jms.TextMessage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.quarkiverse.jms.JmsClient;
import io.quarkiverse.jms.JmsListener;
import io.quarkiverse.jms.JmsQueue;
import io.quarkus.test.QuarkusExtensionTest;

public class JmsCoreTest {

    @RegisterExtension
    static final QuarkusExtensionTest config = new QuarkusExtensionTest()
            .withApplicationRoot(jar -> jar
                    .addClasses(QueueListener.class, TopicListener.class, NoScopeListener.class,
                            SendReceiver.class, TopicReceiver.class,
                            ConnectionFactoryProducer.class, EmbeddedJmsBroker.class)
                    .addAsResource("broker.xml"))
            .setBeforeAllCustomizer(EmbeddedJmsBroker::start)
            .setAfterAllCustomizer(EmbeddedJmsBroker::stop);

    @Inject
    JmsClient jmsClient;

    @Inject
    ConnectionFactory connectionFactory;

    @Inject
    BeanManager beanManager;

    @Inject
    QueueListener queueListener;

    @Inject
    TopicListener topicListener;

    @Inject
    SendReceiver sendReceiver;

    @Inject
    TopicReceiver topicReceiver;

    // --- receive ---

    @Test
    void receiveReturnsMessage() {
        try (JMSContext context = connectionFactory.createContext()) {
            context.createProducer().send(context.createQueue("receive-queue"), "hello-receive");
        }

        Optional<String> result = jmsClient.queue("receive-queue").receive(String.class);
        assertThat(result).isPresent().contains("hello-receive");
    }

    @Test
    void receiveReturnsEmptyOnTimeout() {
        Optional<String> result = jmsClient.queue("empty-queue")
                .receiveTimeout(Duration.ofMillis(500))
                .receive(String.class);
        assertThat(result).isEmpty();
    }

    // --- receive with selector ---

    @Test
    void selectorFiltersOnReceive() throws Exception {
        try (JMSContext context = connectionFactory.createContext()) {
            TextMessage low = context.createTextMessage("low-msg");
            low.setStringProperty("priority", "LOW");
            context.createProducer().send(context.createQueue("selector-recv-queue"), low);

            TextMessage high = context.createTextMessage("high-msg");
            high.setStringProperty("priority", "HIGH");
            context.createProducer().send(context.createQueue("selector-recv-queue"), high);
        }

        Optional<String> result = jmsClient.queue("selector-recv-queue")
                .select("priority = 'HIGH'").receive(String.class);
        assertThat(result).isPresent().contains("high-msg");
    }

    // --- browse ---

    @Test
    void browseReturnsMessagesWithoutConsuming() {
        try (JMSContext context = connectionFactory.createContext()) {
            context.createProducer().send(context.createQueue("browse-queue"), "msg-1");
            context.createProducer().send(context.createQueue("browse-queue"), "msg-2");
            context.createProducer().send(context.createQueue("browse-queue"), "msg-3");
        }

        List<String> browsed = jmsClient.queue("browse-queue", String.class).browse();
        assertThat(browsed).hasSize(3);

        List<String> browsedAgain = jmsClient.queue("browse-queue", String.class).browse();
        assertThat(browsedAgain).hasSize(3);
    }

    // --- queue listener ---

    @Test
    void listenerReceivesMessagesFromQueue() throws Exception {
        Thread.sleep(500);

        try (JMSContext context = connectionFactory.createContext()) {
            context.createProducer().send(context.createQueue("test-queue"), "hello");
        }

        await().atMost(10, TimeUnit.SECONDS)
                .until(() -> queueListener.getMessages().contains("hello"));
    }

    // --- topic listener ---

    @Test
    void listenerReceivesMessagesFromTopic() throws Exception {
        Thread.sleep(500);

        try (JMSContext context = connectionFactory.createContext()) {
            context.createProducer().send(context.createTopic("test-topic"), "topic-msg");
        }

        await().atMost(10, TimeUnit.SECONDS)
                .until(() -> topicListener.getMessages().contains("topic-msg"));
    }

    // --- default scope ---

    @Test
    void listenerWithoutScopeGetsApplicationScope() {
        var beans = beanManager.getBeans(NoScopeListener.class);
        assertThat(beans).isNotEmpty();
        assertThat(beans.iterator().next().getScope()).isEqualTo(ApplicationScoped.class);
    }

    // --- client send ---

    @Test
    void clientSendsMessageReceivedByListener() {
        jmsClient.queue("client-test-queue").send("hello from client");

        await().atMost(10, TimeUnit.SECONDS)
                .until(() -> sendReceiver.getMessages().contains("hello from client"));
    }

    // --- client send topic ---

    @Test
    void clientSendsToTopic() throws Exception {
        Thread.sleep(500);

        jmsClient.topic("client-test-topic").send("topic hello");

        await().atMost(10, TimeUnit.SECONDS)
                .until(() -> topicReceiver.getMessages().contains("topic hello"));
    }

    // --- typed destination ---

    @Test
    void typedQueueSendAndReceive() {
        JmsQueue<String> queue = jmsClient.queue("typed-queue", String.class);
        queue.send("typed-msg");

        Optional<String> result = queue.receive();
        assertThat(result).isPresent().contains("typed-msg");
    }

    @Test
    void typedQueueReceiveNoArgUsesStoredType() {
        JmsQueue<String> queue = jmsClient.queue("typed-noarg-queue", String.class);
        queue.send("default-timeout");

        Optional<String> result = queue.receive();
        assertThat(result).isPresent().contains("default-timeout");
    }

    @Test
    void untypedQueueSendAndReceiveWithExplicitClass() {
        jmsClient.queue("untyped-queue").send("untyped-msg");

        Optional<String> result = jmsClient.queue("untyped-queue").receive(String.class);
        assertThat(result).isPresent().contains("untyped-msg");
    }

    @Test
    void typedQueueExplicitClassOverridesStoredType() {
        JmsQueue<Object> queue = jmsClient.queue("override-queue");
        queue.send("override-msg");

        Optional<String> result = queue.receive(String.class);
        assertThat(result).isPresent().contains("override-msg");
    }

    @Test
    void typedQueueReceiveMessage() {
        JmsQueue<String> queue = jmsClient.queue("raw-typed-queue", String.class);
        queue.send("raw-msg");

        var raw = queue.receiveTimeout(Duration.ofSeconds(5)).receiveMessage();
        assertThat(raw).isPresent();
        assertThat(raw.get().payload()).isEqualTo("raw-msg");
        assertThat(raw.get().message()).isNotNull();
    }

    @Test
    void typedQueueFluentReturnsCorrectType() {
        JmsQueue<String> queue = jmsClient.queue("fluent-queue", String.class);
        queue.message("fluent-msg").property("testProp", "value").priority(5).send();

        Optional<String> result = queue.receive();
        assertThat(result).isPresent().contains("fluent-msg");
    }

    @Test
    void typedQueueBrowse() {
        JmsQueue<String> queue = jmsClient.queue("typed-browse-queue", String.class);
        queue.send("browse-1");
        queue.send("browse-2");

        var browsed = queue.browse();
        assertThat(browsed).hasSize(2).contains("browse-1", "browse-2");
    }

    // --- inner beans ---

    @ApplicationScoped
    public static class QueueListener {

        private final CopyOnWriteArrayList<String> messages = new CopyOnWriteArrayList<>();

        @JmsListener("test-queue")
        public void onMessage(String message) {
            messages.add(message);
        }

        public List<String> getMessages() {
            return messages;
        }
    }

    @ApplicationScoped
    public static class TopicListener {

        private final CopyOnWriteArrayList<String> messages = new CopyOnWriteArrayList<>();

        @JmsListener(value = "test-topic", topic = true)
        public void onMessage(String message) {
            messages.add(message);
        }

        public List<String> getMessages() {
            return messages;
        }
    }

    public static class NoScopeListener {

        @JmsListener("scope-test-queue")
        public void onMessage(String message) {
        }
    }

    @ApplicationScoped
    public static class SendReceiver {

        private final CopyOnWriteArrayList<String> messages = new CopyOnWriteArrayList<>();

        @JmsListener("client-test-queue")
        public void onMessage(String message) {
            messages.add(message);
        }

        public List<String> getMessages() {
            return messages;
        }
    }

    @ApplicationScoped
    public static class TopicReceiver {

        private final CopyOnWriteArrayList<String> messages = new CopyOnWriteArrayList<>();

        @JmsListener(value = "client-test-topic", topic = true)
        public void onMessage(String message) {
            messages.add(message);
        }

        public List<String> getMessages() {
            return messages;
        }
    }
}
