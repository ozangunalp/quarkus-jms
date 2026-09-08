package io.quarkiverse.jms.deployment.test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.JMSContext;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.quarkiverse.jms.JmsListener;
import io.quarkus.test.QuarkusExtensionTest;

public class JmsListenerTransactedTest {

    @RegisterExtension
    static final QuarkusExtensionTest config = new QuarkusExtensionTest()
            .withApplicationRoot(jar -> jar
                    .addClasses(TransactedListener.class, ConnectionFactoryProducer.class,
                            EmbeddedJmsBroker.class)
                    .addAsResource("broker.xml"))
            .setBeforeAllCustomizer(EmbeddedJmsBroker::start)
            .setAfterAllCustomizer(EmbeddedJmsBroker::stop);

    @Inject
    ConnectionFactory connectionFactory;

    @Inject
    TransactedListener listener;

    @Test
    void transactedListenerCommitsOnSuccess() {
        try (JMSContext context = connectionFactory.createContext()) {
            context.createProducer().send(context.createQueue("tx-queue"), "success-msg");
        }

        await().atMost(10, TimeUnit.SECONDS)
                .until(() -> listener.getCommitted().contains("success-msg"));

        assertThat(listener.getCommitted()).containsExactly("success-msg");
    }

    @ApplicationScoped
    public static class TransactedListener {

        private final CopyOnWriteArrayList<String> committed = new CopyOnWriteArrayList<>();
        private final AtomicInteger attempts = new AtomicInteger();

        @JmsListener(value = "tx-queue", acknowledge = JmsListener.AcknowledgeMode.TRANSACTED)
        public void onMessage(String message) {
            committed.add(message);
        }

        public List<String> getCommitted() {
            return committed;
        }
    }
}
