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

public class JmsAcknowledgeModeTest {

    @RegisterExtension
    static final QuarkusExtensionTest config = new QuarkusExtensionTest()
            .withApplicationRoot(jar -> jar
                    .addClasses(AutoAckListener.class, ClientAckListener.class,
                            ClientAckFailListener.class, DupsOkListener.class,
                            ConnectionFactoryProducer.class, EmbeddedJmsBroker.class)
                    .addAsResource("broker.xml"))
            .setBeforeAllCustomizer(EmbeddedJmsBroker::start)
            .setAfterAllCustomizer(EmbeddedJmsBroker::stop);

    @Inject
    ConnectionFactory connectionFactory;

    @Inject
    AutoAckListener autoAckListener;

    @Inject
    ClientAckListener clientAckListener;

    @Inject
    ClientAckFailListener clientAckFailListener;

    @Inject
    DupsOkListener dupsOkListener;

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

    private void send(String queue, String message) {
        try (JMSContext ctx = connectionFactory.createContext()) {
            ctx.createProducer().send(ctx.createQueue(queue), message);
        }
    }

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
}
