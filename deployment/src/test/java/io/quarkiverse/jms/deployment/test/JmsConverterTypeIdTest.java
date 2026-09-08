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
import jakarta.jms.TextMessage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.quarkiverse.jms.JmsClient;
import io.quarkiverse.jms.JmsListener;
import io.quarkus.test.QuarkusExtensionTest;

public class JmsConverterTypeIdTest {

    @RegisterExtension
    static final QuarkusExtensionTest config = new QuarkusExtensionTest()
            .withApplicationRoot(jar -> jar
                    .addClasses(TypeIdListener.class, Product.class,
                            ConnectionFactoryProducer.class, EmbeddedJmsBroker.class)
                    .addAsResource("broker.xml"))
            .overrideRuntimeConfigKey("quarkus.jms.converter.type-id-property", "myType")
            .overrideRuntimeConfigKey("quarkus.jms.converter.type-id-mappings.product",
                    "io.quarkiverse.jms.deployment.test.JmsConverterTypeIdTest$Product")
            .setBeforeAllCustomizer(EmbeddedJmsBroker::start)
            .setAfterAllCustomizer(EmbeddedJmsBroker::stop);

    @Inject
    JmsClient jmsClient;

    @Inject
    ConnectionFactory connectionFactory;

    @Inject
    TypeIdListener listener;

    @Test
    void customTypeIdPropertyIsUsed() throws Exception {
        jmsClient.queue("typeid-queue").send(new Product("widget", 42));

        try (JMSContext context = connectionFactory.createContext()) {
            TextMessage msg = (TextMessage) context.createConsumer(
                    context.createQueue("typeid-queue")).receive(5000);
            assertThat(msg).isNotNull();
            assertThat(msg.getStringProperty("myType")).isEqualTo("product");
        }
    }

    @Test
    void shortTypeNameResolvedOnReceive() throws Exception {
        try (JMSContext context = connectionFactory.createContext()) {
            TextMessage msg = context.createTextMessage("{\"name\":\"gadget\",\"price\":99}");
            msg.setStringProperty("myType", "product");
            context.createProducer().send(context.createQueue("typeid-recv-queue"), msg);
        }

        await().atMost(10, TimeUnit.SECONDS)
                .until(() -> !listener.getProducts().isEmpty());

        assertThat(listener.getProducts().get(0).name).isEqualTo("gadget");
        assertThat(listener.getProducts().get(0).price).isEqualTo(99);
    }

    @ApplicationScoped
    public static class TypeIdListener {

        private final CopyOnWriteArrayList<Product> products = new CopyOnWriteArrayList<>();

        @JmsListener("typeid-recv-queue")
        public void onProduct(Product product) {
            products.add(product);
        }

        public List<Product> getProducts() {
            return products;
        }
    }

    public static class Product {
        public String name;
        public int price;

        public Product() {
        }

        public Product(String name, int price) {
            this.name = name;
            this.price = price;
        }
    }
}
