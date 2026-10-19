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
import io.smallrye.common.annotation.RunOnVirtualThread;

public class JmsListenerVirtualThreadScalingTest {

    @RegisterExtension
    static final QuarkusExtensionTest config = new QuarkusExtensionTest()
            .overrideConfigKey("quarkus.http.test-port", "0")
            .withApplicationRoot(jar -> jar
                    .addClasses(VtScalingListener.class, ConnectionFactoryProducer.class,
                            EmbeddedJmsBroker.class)
                    .addAsResource("broker.xml"))
            .setBeforeAllCustomizer(EmbeddedJmsBroker::start)
            .setAfterAllCustomizer(EmbeddedJmsBroker::stop)
            .overrideConfigKey("quarkus.jms.receive-timeout", "PT1S")
            .overrideConfigKey("quarkus.jms.listener.scale-down-idle-timeout", "PT3S");

    @Inject
    ConnectionFactory connectionFactory;

    @Inject
    VtScalingListener listener;

    @Test
    void scalesUpOnVirtualThreadsAndScalesDown() {
        listener.setProcessingDelay(500);

        try (JMSContext ctx = connectionFactory.createContext()) {
            for (int i = 0; i < 50; i++) {
                ctx.createProducer().send(ctx.createQueue("vt-scaling-queue"), "burst-" + i);
            }
        }

        await().atMost(30, TimeUnit.SECONDS)
                .until(() -> listener.getReceived().size() >= 50);

        assertThat(listener.getReceived()).hasSize(50);
        assertThat(listener.getMaxConcurrentCount()).as("Should scale up beyond min concurrency")
                .isGreaterThan(1);
        assertThat(listener.getThreadNames()).allSatisfy(name -> assertThat(name).contains("quarkus-virtual-thread"));

        listener.setProcessingDelay(0);

        int peakConcurrent = listener.getMaxConcurrentCount();

        await().atMost(15, TimeUnit.SECONDS)
                .pollInterval(1, TimeUnit.SECONDS)
                .untilAsserted(() -> {
                    try (JMSContext ctx = connectionFactory.createContext()) {
                        ctx.createProducer().send(ctx.createQueue("vt-scaling-queue"), "probe");
                    }
                    assertThat(listener.getActiveConcurrentCount())
                            .as("Should scale down after idle period")
                            .isLessThan(peakConcurrent);
                });
    }

    @ApplicationScoped
    public static class VtScalingListener {

        private final CopyOnWriteArraySet<String> received = new CopyOnWriteArraySet<>();
        private final Set<String> threadNames = new CopyOnWriteArraySet<>();
        private final AtomicInteger concurrentCount = new AtomicInteger();
        private final AtomicInteger maxConcurrent = new AtomicInteger();
        private volatile int processingDelay = 0;

        @JmsListener(value = "vt-scaling-queue", maxConcurrency = 5)
        @RunOnVirtualThread
        public void onMessage(String message) {
            String tName = Thread.currentThread().getName();
            threadNames.add(tName);
            int current = concurrentCount.incrementAndGet();
            int maxC = maxConcurrent.updateAndGet(max -> Math.max(max, current));
            System.out.println(tName + " processing " + message + " (concurrent: " + current + "/ " + maxC + ")");
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

        public Set<String> getThreadNames() {
            return threadNames;
        }

        public int getMaxConcurrentCount() {
            return maxConcurrent.get();
        }

        public int getActiveConcurrentCount() {
            return concurrentCount.get();
        }

        public void setProcessingDelay(int millis) {
            this.processingDelay = millis;
        }
    }
}
