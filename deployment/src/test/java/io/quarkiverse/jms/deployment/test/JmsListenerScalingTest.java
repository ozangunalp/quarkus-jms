package io.quarkiverse.jms.deployment.test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
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

public class JmsListenerScalingTest {

    @RegisterExtension
    static final QuarkusExtensionTest config = new QuarkusExtensionTest()
            .withApplicationRoot(jar -> jar
                    .addClasses(ScalingListener.class, ConnectionFactoryProducer.class,
                            EmbeddedJmsBroker.class)
                    .addAsResource("broker.xml"))
            .setBeforeAllCustomizer(EmbeddedJmsBroker::start)
            .setAfterAllCustomizer(EmbeddedJmsBroker::stop)
            .overrideConfigKey("quarkus.jms.receive-timeout", "PT1S")
            .overrideConfigKey("quarkus.jms.listener.scale-down-idle-timeout", "PT3S");

    @Inject
    ConnectionFactory connectionFactory;

    @Inject
    ScalingListener listener;

    @Test
    void scalesUpUnderLoadAndDownWhenIdle() throws Exception {
        listener.setProcessingDelay(500);

        try (JMSContext ctx = connectionFactory.createContext()) {
            for (int i = 0; i < 10; i++) {
                ctx.createProducer().send(ctx.createQueue("scaling-queue"), "burst-" + i);
            }
        }

        await().atMost(15, TimeUnit.SECONDS)
                .until(() -> listener.getReceived().size() >= 10);

        assertThat(listener.getReceived()).hasSize(10);
        assertThat(listener.getMaxConcurrentThreads()).as("Should scale up beyond min concurrency")
                .isGreaterThan(1);

        listener.setProcessingDelay(0);

        int peakThreads = listener.getMaxConcurrentThreads();

        await().atMost(15, TimeUnit.SECONDS)
                .pollInterval(1, TimeUnit.SECONDS)
                .untilAsserted(() -> {
                    try (JMSContext ctx = connectionFactory.createContext()) {
                        ctx.createProducer().send(ctx.createQueue("scaling-probe-queue"), "probe");
                    }
                    assertThat(listener.getActiveThreadCount())
                            .as("Should scale down after idle period")
                            .isLessThan(peakThreads);
                });
    }

    @ApplicationScoped
    public static class ScalingListener {

        private final CopyOnWriteArraySet<String> received = new CopyOnWriteArraySet<>();
        private final AtomicInteger concurrentCount = new AtomicInteger();
        private final AtomicInteger maxConcurrent = new AtomicInteger();
        private final Set<String> activeThreads = new CopyOnWriteArraySet<>();
        private volatile int processingDelay = 0;

        @JmsListener(value = "scaling-queue", concurrency = 1, maxConcurrency = 5)
        public void onMessage(String message) {
            activeThreads.add(Thread.currentThread().getName());
            int current = concurrentCount.incrementAndGet();
            maxConcurrent.updateAndGet(max -> Math.max(max, current));
            try {
                if (processingDelay > 0) {
                    Thread.sleep(processingDelay);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                concurrentCount.decrementAndGet();
            }
            received.add(message);
        }

        public Set<String> getReceived() {
            return received;
        }

        public int getMaxConcurrentThreads() {
            return maxConcurrent.get();
        }

        public int getActiveThreadCount() {
            return concurrentCount.get();
        }

        public void setProcessingDelay(int millis) {
            this.processingDelay = millis;
        }
    }
}
