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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.quarkiverse.jms.JmsClient;
import io.quarkiverse.jms.JmsListener;
import io.quarkus.test.QuarkusExtensionTest;

public class JmsMetricsTest {

    @RegisterExtension
    static final QuarkusExtensionTest config = new QuarkusExtensionTest()
            .withApplicationRoot(jar -> jar
                    .addClasses(MetricsListener.class, ConnectionFactoryProducer.class,
                            EmbeddedJmsBroker.class)
                    .addAsResource("broker.xml"))
            .setBeforeAllCustomizer(EmbeddedJmsBroker::start)
            .setAfterAllCustomizer(EmbeddedJmsBroker::stop);

    @Inject
    JmsClient jmsClient;

    @Inject
    ConnectionFactory connectionFactory;

    @Inject
    MeterRegistry registry;

    @Inject
    MetricsListener listener;

    @Test
    void sendMetricsRecorded() {
        jmsClient.queue("metrics-send-queue").send("test-msg");

        Timer sendTimer = registry.find("jms.send")
                .tag("destination", "metrics-send-queue")
                .tag("outcome", "success")
                .timer();

        assertThat(sendTimer).isNotNull();
        assertThat(sendTimer.count()).isEqualTo(1);
    }

    @Test
    void receiveMetricsRecorded() {
        try (JMSContext ctx = connectionFactory.createContext()) {
            ctx.createProducer().send(ctx.createQueue("metrics-recv-queue"), "recv-msg");
        }

        jmsClient.queue("metrics-recv-queue", String.class).receive();

        Timer recvTimer = registry.find("jms.receive")
                .tag("destination", "metrics-recv-queue")
                .tag("outcome", "received")
                .timer();

        assertThat(recvTimer).isNotNull();
        assertThat(recvTimer.count()).isEqualTo(1);
    }

    @Test
    void listenerMetricsRecorded() {
        try (JMSContext ctx = connectionFactory.createContext()) {
            ctx.createProducer().send(ctx.createQueue("metrics-listener-queue"), "listener-msg");
        }

        await().atMost(10, TimeUnit.SECONDS)
                .until(() -> !listener.getReceived().isEmpty());

        Timer listenerTimer = registry.find("jms.listener")
                .tag("destination", "metrics-listener-queue")
                .tag("outcome", "success")
                .timer();

        assertThat(listenerTimer).isNotNull();
        assertThat(listenerTimer.count()).isGreaterThanOrEqualTo(1);
    }

    @ApplicationScoped
    public static class MetricsListener {

        private final List<String> received = new CopyOnWriteArrayList<>();

        @JmsListener("metrics-listener-queue")
        public void onMessage(String message) {
            received.add(message);
        }

        public List<String> getReceived() {
            return received;
        }
    }
}
