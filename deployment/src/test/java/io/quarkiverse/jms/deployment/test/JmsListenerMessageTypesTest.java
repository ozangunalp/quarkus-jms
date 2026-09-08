package io.quarkiverse.jms.deployment.test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.JMSContext;
import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.TextMessage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.quarkiverse.jms.JmsClient;
import io.quarkiverse.jms.JmsListener;
import io.quarkus.test.QuarkusExtensionTest;

public class JmsListenerMessageTypesTest {

    @RegisterExtension
    static final QuarkusExtensionTest config = new QuarkusExtensionTest()
            .withApplicationRoot(jar -> jar
                    .addClasses(OrderWithMessageListener.class, ByteArrayListener.class,
                            PojoListener.class, MultiDestListener.class,
                            Order.class,
                            ConnectionFactoryProducer.class, EmbeddedJmsBroker.class)
                    .addAsResource("broker.xml"))
            .setBeforeAllCustomizer(EmbeddedJmsBroker::start)
            .setAfterAllCustomizer(EmbeddedJmsBroker::stop);

    @Inject
    JmsClient jmsClient;

    @Inject
    ConnectionFactory connectionFactory;

    @Inject
    OrderWithMessageListener orderListener;

    @Inject
    ByteArrayListener byteArrayListener;

    @Inject
    PojoListener pojoListener;

    @Inject
    MultiDestListener multiDestListener;

    // --- raw message ---

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

    // --- byte array ---

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

    // --- JSON POJO ---

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

    // --- repeatable ---

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

    // --- inner beans ---

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
}
