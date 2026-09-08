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

import io.quarkiverse.jms.JmsListener;
import io.quarkus.test.QuarkusExtensionTest;

public class JmsListenerSelectorTest {

    @RegisterExtension
    static final QuarkusExtensionTest config = new QuarkusExtensionTest()
            .withApplicationRoot(jar -> jar
                    .addClasses(SelectorListener.class, ConnectionFactoryProducer.class,
                            EmbeddedJmsBroker.class)
                    .addAsResource("broker.xml"))
            .setBeforeAllCustomizer(EmbeddedJmsBroker::start)
            .setAfterAllCustomizer(EmbeddedJmsBroker::stop);

    @Inject
    ConnectionFactory connectionFactory;

    @Inject
    SelectorListener listener;

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
                .until(() -> !listener.getMessages().isEmpty());

        // Wait a bit more to make sure the LOW message is NOT received
        Thread.sleep(1000);

        assertThat(listener.getMessages()).containsExactly("high-priority");
    }

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
}
