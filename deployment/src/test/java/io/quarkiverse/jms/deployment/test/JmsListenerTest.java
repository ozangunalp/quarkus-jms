package io.quarkiverse.jms.deployment.test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.spi.BeanManager;
import jakarta.inject.Inject;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.JMSContext;
import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.TextMessage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.quarkiverse.jms.JmsClient;
import io.quarkiverse.jms.JmsHeader;
import io.quarkiverse.jms.JmsListener;
import io.quarkiverse.jms.JmsQueue;
import io.quarkus.test.QuarkusExtensionTest;

public class JmsListenerTest {

    @RegisterExtension
    static final QuarkusExtensionTest config = new QuarkusExtensionTest()
            .overrideConfigKey("quarkus.http.test-port", "0")
            .withApplicationRoot(jar -> jar
                    .addClasses(
                            // core listeners
                            QueueListener.class, TopicListener.class, NoScopeListener.class,
                            SendReceiver.class, TopicReceiver.class,
                            // acknowledge mode listeners
                            AutoAckListener.class, ClientAckListener.class,
                            ClientAckFailListener.class, DupsOkListener.class,
                            // manual client ack listener
                            ManualClientAckListener.class,
                            // concurrency listeners
                            ConcurrentListener.class, MaxConcurrencyListener.class,
                            // message type listeners
                            OrderWithMessageListener.class, ByteArrayListener.class,
                            PojoListener.class, MultiDestListener.class, Order.class,
                            // selector listener
                            SelectorListener.class,
                            // transacted listener
                            TransactedListener.class,
                            // header listener
                            HeaderListener.class,
                            // infrastructure
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

    @Inject
    AutoAckListener autoAckListener;

    @Inject
    ClientAckListener clientAckListener;

    @Inject
    ClientAckFailListener clientAckFailListener;

    @Inject
    DupsOkListener dupsOkListener;

    @Inject
    ManualClientAckListener manualClientAckListener;

    @Inject
    ConcurrentListener concurrentListener;

    @Inject
    MaxConcurrencyListener maxConcurrencyListener;

    @Inject
    OrderWithMessageListener orderListener;

    @Inject
    ByteArrayListener byteArrayListener;

    @Inject
    PojoListener pojoListener;

    @Inject
    MultiDestListener multiDestListener;

    @Inject
    SelectorListener selectorListener;

    @Inject
    TransactedListener transactedListener;

    @Inject
    HeaderListener headerListener;

    // ==================== core: receive ====================

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

    // ==================== core: browse ====================

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

    // ==================== core: queue listener ====================

    @Test
    void listenerReceivesMessagesFromQueue() throws Exception {
        Thread.sleep(500);

        try (JMSContext context = connectionFactory.createContext()) {
            context.createProducer().send(context.createQueue("test-queue"), "hello");
        }

        await().atMost(10, TimeUnit.SECONDS)
                .until(() -> queueListener.getMessages().contains("hello"));
    }

    // ==================== core: topic listener ====================

    @Test
    void listenerReceivesMessagesFromTopic() throws Exception {
        Thread.sleep(500);

        try (JMSContext context = connectionFactory.createContext()) {
            context.createProducer().send(context.createTopic("test-topic"), "topic-msg");
        }

        await().atMost(10, TimeUnit.SECONDS)
                .until(() -> topicListener.getMessages().contains("topic-msg"));
    }

    // ==================== core: default scope ====================

    @Test
    void listenerWithoutScopeGetsApplicationScope() {
        var beans = beanManager.getBeans(NoScopeListener.class);
        assertThat(beans).isNotEmpty();
        assertThat(beans.iterator().next().getScope()).isEqualTo(ApplicationScoped.class);
    }

    // ==================== core: client send ====================

    @Test
    void clientSendsMessageReceivedByListener() {
        jmsClient.queue("client-test-queue").send("hello from client");

        await().atMost(10, TimeUnit.SECONDS)
                .until(() -> sendReceiver.getMessages().contains("hello from client"));
    }

    @Test
    void clientSendsToTopic() throws Exception {
        Thread.sleep(500);

        jmsClient.topic("client-test-topic").send("topic hello");

        await().atMost(10, TimeUnit.SECONDS)
                .until(() -> topicReceiver.getMessages().contains("topic hello"));
    }

    // ==================== core: typed destination ====================

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

    // ==================== acknowledge mode ====================

    @Test
    void autoAcknowledgeProcessesMessages() {
        send("auto-ack-queue", "auto-msg");

        await().atMost(10, TimeUnit.SECONDS)
                .until(() -> autoAckListener.getReceived().contains("auto-msg"));
    }

    @Test
    void clientAcknowledgeAutoAcksOnSuccess() {
        send("client-ack-success-queue", "client-msg");

        await().atMost(10, TimeUnit.SECONDS)
                .until(() -> clientAckListener.getReceived().contains("client-msg"));

        assertThat(clientAckListener.getAttempts()).isEqualTo(1);
    }

    @Test
    void clientAcknowledgeRecoversOnFailure() {
        send("client-ack-fail-queue", "fail-msg");

        await().atMost(10, TimeUnit.SECONDS)
                .until(() -> clientAckFailListener.getAttempts() >= 2);

        assertThat(clientAckFailListener.getReceived()).contains("fail-msg");
    }

    @Test
    void dupsOkAcknowledgeProcessesMessages() {
        send("dups-ok-queue", "dups-msg");

        await().atMost(10, TimeUnit.SECONDS)
                .until(() -> dupsOkListener.getReceived().contains("dups-msg"));
    }

    // ==================== manual client acknowledge ====================

    @Test
    void clientAcknowledgeModeReceivesMessages() {
        try (JMSContext ctx = connectionFactory.createContext()) {
            ctx.createProducer().send(ctx.createQueue("manual-client-ack-queue"), "ack-me");
        }

        await().atMost(10, TimeUnit.SECONDS)
                .until(() -> !manualClientAckListener.getReceived().isEmpty());

        assertThat(manualClientAckListener.getReceived()).containsExactly("ack-me");
    }

    // ==================== concurrency ====================

    @Test
    void concurrentConsumersProcessMessages() {
        try (JMSContext context = connectionFactory.createContext()) {
            for (int i = 0; i < 10; i++) {
                context.createProducer().send(context.createQueue("concurrent-queue"),
                        "msg-" + i);
            }
        }

        await().atMost(10, TimeUnit.SECONDS)
                .until(() -> concurrentListener.getReceived().size() >= 10);

        assertThat(concurrentListener.getReceived()).hasSize(10);
        assertThat(concurrentListener.getThreadNames().size()).as("Multiple threads should process messages")
                .isGreaterThan(1);
    }

    // ==================== max concurrency ====================

    @Test
    void maxConcurrencyAttributeIsAccepted() {
        try (JMSContext context = connectionFactory.createContext()) {
            for (int i = 0; i < 5; i++) {
                context.createProducer().send(context.createQueue("max-conc-queue"), "msg-" + i);
            }
        }

        await().atMost(10, TimeUnit.SECONDS)
                .until(() -> maxConcurrencyListener.getReceived().size() >= 5);

        assertThat(maxConcurrencyListener.getReceived()).hasSize(5);
    }

    // ==================== message types ====================

    @Test
    void listenerReceivesPayloadAndRawMessage() {
        jmsClient.queue("raw-msg-queue").message(new Order(1, "widget"))
                .property("region", "EU")
                .send();

        await().atMost(10, TimeUnit.SECONDS)
                .until(() -> !orderListener.getRecords().isEmpty());

        var record = orderListener.getRecords().get(0);
        assertThat(record.order.id).isEqualTo(1);
        assertThat(record.order.product).isEqualTo("widget");
        assertThat(record.region).isEqualTo("EU");
    }

    @Test
    void listenerReceivesByteArrayPayload() {
        byte[] payload = "binary-content".getBytes();
        jmsClient.queue("byte-array-queue").send(payload);

        await().atMost(10, TimeUnit.SECONDS)
                .untilAsserted(() -> {
                    assertThat(byteArrayListener.getReceived()).hasSize(1);
                    assertThat(new String(byteArrayListener.getReceived().get(0)))
                            .isEqualTo("binary-content");
                });
    }

    @Test
    void listenerDeserializesJsonPojo() throws Exception {
        try (JMSContext context = connectionFactory.createContext()) {
            TextMessage msg = context.createTextMessage("{\"id\":42,\"product\":\"widget\"}");
            msg.setStringProperty("_type", Order.class.getName());
            context.createProducer().send(context.createQueue("orders"), msg);
        }

        await().atMost(10, TimeUnit.SECONDS)
                .until(() -> !pojoListener.getOrders().isEmpty());

        Order order = pojoListener.getOrders().get(0);
        assertThat(order.id).isEqualTo(42);
        assertThat(order.product).isEqualTo("widget");
    }

    @Test
    void sameMethodReceivesFromMultipleDestinations() {
        try (JMSContext context = connectionFactory.createContext()) {
            context.createProducer().send(context.createQueue("repeat-queue-1"), "from-q1");
            context.createProducer().send(context.createQueue("repeat-queue-2"), "from-q2");
        }

        await().atMost(10, TimeUnit.SECONDS)
                .until(() -> multiDestListener.getMessages().size() >= 2);

        assertThat(multiDestListener.getMessages()).contains("from-q1", "from-q2");
    }

    // ==================== selector ====================

    @Test
    void selectorFiltersMessages() throws Exception {
        try (JMSContext context = connectionFactory.createContext()) {
            TextMessage high = context.createTextMessage("high-priority");
            high.setStringProperty("priority", "HIGH");
            context.createProducer().send(context.createQueue("filtered-queue"), high);

            TextMessage low = context.createTextMessage("low-priority");
            low.setStringProperty("priority", "LOW");
            context.createProducer().send(context.createQueue("filtered-queue"), low);
        }

        await().atMost(10, TimeUnit.SECONDS)
                .until(() -> !selectorListener.getMessages().isEmpty());

        Thread.sleep(1000);

        assertThat(selectorListener.getMessages()).containsExactly("high-priority");
    }

    // ==================== transacted ====================

    @Test
    void transactedListenerCommitsOnSuccess() {
        try (JMSContext context = connectionFactory.createContext()) {
            context.createProducer().send(context.createQueue("tx-queue"), "success-msg");
        }

        await().atMost(10, TimeUnit.SECONDS)
                .until(() -> transactedListener.getCommitted().contains("success-msg"));

        assertThat(transactedListener.getCommitted()).containsExactly("success-msg");
    }

    // ==================== header annotation ====================

    @Test
    void jmsHeaderInjectedIntoMethodParameter() throws Exception {
        try (JMSContext context = connectionFactory.createContext()) {
            TextMessage msg = context.createTextMessage("header-payload");
            msg.setStringProperty("source", "test-system");
            context.createProducer().send(context.createQueue("header-queue"), msg);
        }

        await().atMost(10, TimeUnit.SECONDS)
                .until(() -> !headerListener.getRecords().isEmpty());

        assertThat(headerListener.getRecords().get(0)).isEqualTo("header-payload|test-system");
    }

    // ==================== helpers ====================

    private void send(String queue, String message) {
        try (JMSContext ctx = connectionFactory.createContext()) {
            ctx.createProducer().send(ctx.createQueue(queue), message);
        }
    }

    // ==================== inner beans: core ====================

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

    // ==================== inner beans: acknowledge mode ====================

    @ApplicationScoped
    public static class AutoAckListener {
        private final List<String> received = new CopyOnWriteArrayList<>();

        @JmsListener(value = "auto-ack-queue", acknowledge = JmsListener.AcknowledgeMode.AUTO)
        public void onMessage(String message) {
            received.add(message);
        }

        public List<String> getReceived() {
            return received;
        }
    }

    @ApplicationScoped
    public static class ClientAckListener {
        private final List<String> received = new CopyOnWriteArrayList<>();
        private final AtomicInteger attempts = new AtomicInteger();

        @JmsListener(value = "client-ack-success-queue", acknowledge = JmsListener.AcknowledgeMode.CLIENT)
        public void onMessage(String message) {
            attempts.incrementAndGet();
            received.add(message);
        }

        public List<String> getReceived() {
            return received;
        }

        public int getAttempts() {
            return attempts.get();
        }
    }

    @ApplicationScoped
    public static class ClientAckFailListener {
        private final List<String> received = new CopyOnWriteArrayList<>();
        private final AtomicInteger attempts = new AtomicInteger();

        @JmsListener(value = "client-ack-fail-queue", acknowledge = JmsListener.AcknowledgeMode.CLIENT)
        public void onMessage(String message) {
            int attempt = attempts.incrementAndGet();
            if (attempt == 1) {
                throw new RuntimeException("Intentional failure on first attempt");
            }
            received.add(message);
        }

        public List<String> getReceived() {
            return received;
        }

        public int getAttempts() {
            return attempts.get();
        }
    }

    @ApplicationScoped
    public static class DupsOkListener {
        private final List<String> received = new CopyOnWriteArrayList<>();

        @JmsListener(value = "dups-ok-queue", acknowledge = JmsListener.AcknowledgeMode.DUPS_OK)
        public void onMessage(String message) {
            received.add(message);
        }

        public List<String> getReceived() {
            return received;
        }
    }

    // ==================== inner beans: manual client ack ====================

    @ApplicationScoped
    public static class ManualClientAckListener {

        private final List<String> received = new CopyOnWriteArrayList<>();

        @JmsListener(value = "manual-client-ack-queue", acknowledge = JmsListener.AcknowledgeMode.CLIENT)
        public void onMessage(Message message) throws JMSException {
            received.add(message.getBody(String.class));
            message.acknowledge();
        }

        public List<String> getReceived() {
            return received;
        }
    }

    // ==================== inner beans: concurrency ====================

    @ApplicationScoped
    public static class ConcurrentListener {

        private final CopyOnWriteArraySet<String> received = new CopyOnWriteArraySet<>();
        private final Set<String> threadNames = new CopyOnWriteArraySet<>();

        @JmsListener(value = "concurrent-queue", concurrency = 3)
        public void onMessage(String message) {
            threadNames.add(Thread.currentThread().getName());
            received.add(message);
        }

        public Set<String> getReceived() {
            return received;
        }

        public Set<String> getThreadNames() {
            return threadNames;
        }
    }

    @ApplicationScoped
    public static class MaxConcurrencyListener {

        private final CopyOnWriteArraySet<String> received = new CopyOnWriteArraySet<>();

        @JmsListener(value = "max-conc-queue", concurrency = 2, maxConcurrency = 5)
        public void onMessage(String message) {
            received.add(message);
        }

        public Set<String> getReceived() {
            return received;
        }
    }

    // ==================== inner beans: message types ====================

    @ApplicationScoped
    public static class OrderWithMessageListener {

        private final CopyOnWriteArrayList<OrderRecord> records = new CopyOnWriteArrayList<>();

        @JmsListener("raw-msg-queue")
        public void onOrder(Order order, Message message) throws JMSException {
            records.add(new OrderRecord(order, message.getStringProperty("region")));
        }

        public List<OrderRecord> getRecords() {
            return records;
        }
    }

    public record OrderRecord(Order order, String region) {
    }

    @ApplicationScoped
    public static class ByteArrayListener {

        private final List<byte[]> received = new CopyOnWriteArrayList<>();

        @JmsListener("byte-array-queue")
        public void onMessage(byte[] data) {
            received.add(data);
        }

        public List<byte[]> getReceived() {
            return received;
        }
    }

    @ApplicationScoped
    public static class PojoListener {

        private final CopyOnWriteArrayList<Order> orders = new CopyOnWriteArrayList<>();

        @JmsListener("orders")
        public void onOrder(Order order) {
            orders.add(order);
        }

        public List<Order> getOrders() {
            return orders;
        }
    }

    @ApplicationScoped
    public static class MultiDestListener {

        private final CopyOnWriteArrayList<String> messages = new CopyOnWriteArrayList<>();

        @JmsListener("repeat-queue-1")
        @JmsListener("repeat-queue-2")
        public void onMessage(String message) {
            messages.add(message);
        }

        public List<String> getMessages() {
            return messages;
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

    // ==================== inner beans: selector ====================

    @ApplicationScoped
    public static class SelectorListener {

        private final CopyOnWriteArrayList<String> messages = new CopyOnWriteArrayList<>();

        @JmsListener(value = "filtered-queue", selector = "priority = 'HIGH'")
        public void onMessage(String message) {
            messages.add(message);
        }

        public List<String> getMessages() {
            return messages;
        }
    }

    // ==================== inner beans: transacted ====================

    @ApplicationScoped
    public static class TransactedListener {

        private final CopyOnWriteArrayList<String> committed = new CopyOnWriteArrayList<>();

        @JmsListener(value = "tx-queue", acknowledge = JmsListener.AcknowledgeMode.TRANSACTED)
        public void onMessage(String message) {
            committed.add(message);
        }

        public List<String> getCommitted() {
            return committed;
        }
    }

    // ==================== inner beans: header ====================

    @ApplicationScoped
    public static class HeaderListener {

        private final CopyOnWriteArrayList<String> records = new CopyOnWriteArrayList<>();

        @JmsListener("header-queue")
        public void onMessage(String payload, @JmsHeader("source") String source) {
            records.add(payload + "|" + source);
        }

        public List<String> getRecords() {
            return records;
        }
    }
}
