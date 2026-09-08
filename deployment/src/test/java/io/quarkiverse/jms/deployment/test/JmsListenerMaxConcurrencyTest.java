package io.quarkiverse.jms.deployment.test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.TimeUnit;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.JMSContext;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.quarkiverse.jms.JmsListener;
import io.quarkus.test.QuarkusExtensionTest;

public class JmsListenerMaxConcurrencyTest {

    @RegisterExtension
    static final QuarkusExtensionTest config = new QuarkusExtensionTest()
            .withApplicationRoot(jar -> jar
                    .addClasses(MaxConcurrencyListener.class, ConnectionFactoryProducer.class,
                            EmbeddedJmsBroker.class)
                    .addAsResource("broker.xml"))
            .setBeforeAllCustomizer(EmbeddedJmsBroker::start)
            .setAfterAllCustomizer(EmbeddedJmsBroker::stop);

    @Inject
    ConnectionFactory connectionFactory;

    @Inject
    MaxConcurrencyListener listener;

    @Test
    void maxConcurrencyAttributeIsAccepted() {
        try (JMSContext context = connectionFactory.createContext()) {
            for (int i = 0; i < 5; i++) {
                context.createProducer().send(context.createQueue("max-conc-queue"), "msg-" + i);
            }
        }

        await().atMost(10, TimeUnit.SECONDS)
                .until(() -> listener.getReceived().size() >= 5);

        assertThat(listener.getReceived()).hasSize(5);
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
}
