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

public class JmsListenerConcurrencyTest {

    @RegisterExtension
    static final QuarkusExtensionTest config = new QuarkusExtensionTest()
            .withApplicationRoot(jar -> jar
                    .addClasses(ConcurrentListener.class, ConnectionFactoryProducer.class,
                            EmbeddedJmsBroker.class)
                    .addAsResource("broker.xml"))
            .setBeforeAllCustomizer(EmbeddedJmsBroker::start)
            .setAfterAllCustomizer(EmbeddedJmsBroker::stop);

    @Inject
    ConnectionFactory connectionFactory;

    @Inject
    ConcurrentListener listener;

    @Test
    void concurrentConsumersProcessMessages() {
        try (JMSContext context = connectionFactory.createContext()) {
            for (int i = 0; i < 10; i++) {
                context.createProducer().send(context.createQueue("concurrent-queue"),
                        "msg-" + i);
            }
        }

        await().atMost(10, TimeUnit.SECONDS)
                .until(() -> listener.getReceived().size() >= 10);

        assertThat(listener.getReceived()).hasSize(10);
        assertThat(listener.getThreadNames().size()).as("Multiple threads should process messages")
                .isGreaterThan(1);
    }

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
}
