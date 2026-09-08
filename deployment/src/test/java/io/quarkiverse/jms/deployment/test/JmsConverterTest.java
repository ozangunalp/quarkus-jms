package io.quarkiverse.jms.deployment.test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;

import jakarta.inject.Inject;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.JMSContext;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.quarkiverse.jms.JmsClient;
import io.quarkiverse.jms.JmsConversionException;
import io.quarkiverse.jms.ReceiveMessage;
import io.quarkus.test.QuarkusExtensionTest;

public class JmsConverterTest {

    @RegisterExtension
    static final QuarkusExtensionTest config = new QuarkusExtensionTest()
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

    // --- converter resolution ---

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

    // --- untyped receive ---

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

    // --- conversion errors ---

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

    // --- inner types ---

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
