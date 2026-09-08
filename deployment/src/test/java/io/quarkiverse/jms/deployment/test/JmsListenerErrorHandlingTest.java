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
import jakarta.jms.Message;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.quarkiverse.jms.JmsConversionException;
import io.quarkiverse.jms.JmsListener;
import io.quarkiverse.jms.JmsListenerErrorHandler;
import io.quarkus.test.QuarkusExtensionTest;

public class JmsListenerErrorHandlingTest {

    @RegisterExtension
    static final QuarkusExtensionTest config = new QuarkusExtensionTest()
            .withApplicationRoot(jar -> jar
                    .addClasses(FailingListener.class, PojoListener.class,
                            SharedErrorHandler.class, Order.class,
                            ConnectionFactoryProducer.class, EmbeddedJmsBroker.class)
                    .addAsResource("broker.xml"))
            .setBeforeAllCustomizer(EmbeddedJmsBroker::start)
            .setAfterAllCustomizer(EmbeddedJmsBroker::stop);

    @Inject
    ConnectionFactory connectionFactory;

    @Inject
    SharedErrorHandler errorHandler;

    @Inject
    PojoListener pojoListener;

    @Test
    void errorHandlerInvokedOnListenerException() {
        try (JMSContext ctx = connectionFactory.createContext()) {
            ctx.createProducer().send(ctx.createQueue("error-handler-queue"), "boom");
        }

        await().atMost(10, TimeUnit.SECONDS)
                .until(() -> errorHandler.getErrors().stream()
                        .anyMatch(e -> "error-handler-queue".equals(e.destination)));

        var error = errorHandler.getErrors().stream()
                .filter(e -> "error-handler-queue".equals(e.destination))
                .findFirst().orElseThrow();
        assertThat(error.error).isInstanceOf(RuntimeException.class);
        assertThat(error.error).hasMessage("listener failed");
        assertThat(error.message).isNotNull();
    }

    @Test
    void invalidJsonTriggersConversionException() {
        try (JMSContext ctx = connectionFactory.createContext()) {
            ctx.createProducer().send(ctx.createQueue("conv-listener-queue"),
                    ctx.createTextMessage("not valid json"));
        }

        await().atMost(10, TimeUnit.SECONDS)
                .until(() -> errorHandler.getErrors().stream()
                        .anyMatch(e -> "conv-listener-queue".equals(e.destination)));

        var error = errorHandler.getErrors().stream()
                .filter(e -> "conv-listener-queue".equals(e.destination))
                .findFirst().orElseThrow();
        assertThat(error.error).isInstanceOf(JmsConversionException.class);
        assertThat(error.message).isNotNull();
    }

    @Test
    void validJsonProcessedSuccessfully() throws Exception {
        try (JMSContext ctx = connectionFactory.createContext()) {
            var msg = ctx.createTextMessage("{\"id\":42,\"product\":\"widget\"}");
            msg.setStringProperty("_type", Order.class.getName());
            ctx.createProducer().send(ctx.createQueue("conv-listener-queue"), msg);
        }

        await().atMost(10, TimeUnit.SECONDS)
                .until(() -> !pojoListener.getOrders().isEmpty());

        assertThat(pojoListener.getOrders().get(0).id).isEqualTo(42);
    }

    // --- inner beans ---

    @ApplicationScoped
    public static class FailingListener {

        @JmsListener("error-handler-queue")
        public void onMessage(String message) {
            throw new RuntimeException("listener failed");
        }
    }

    @ApplicationScoped
    public static class PojoListener {

        private final CopyOnWriteArrayList<Order> orders = new CopyOnWriteArrayList<>();

        @JmsListener("conv-listener-queue")
        public void onOrder(Order order) {
            orders.add(order);
        }

        public List<Order> getOrders() {
            return orders;
        }
    }

    @ApplicationScoped
    public static class SharedErrorHandler implements JmsListenerErrorHandler {

        private final List<ErrorRecord> errors = new CopyOnWriteArrayList<>();

        @Override
        public void handleError(JMSContext context, Message message, String destination, Throwable error) {
            errors.add(new ErrorRecord(message, destination, error));
        }

        public List<ErrorRecord> getErrors() {
            return errors;
        }
    }

    public record ErrorRecord(Message message, String destination, Throwable error) {
    }

    public static class Order {
        public int id;
        public String product;
    }
}
