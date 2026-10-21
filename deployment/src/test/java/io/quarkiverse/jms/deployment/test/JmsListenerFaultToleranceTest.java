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

import org.eclipse.microprofile.faulttolerance.Fallback;
import org.eclipse.microprofile.faulttolerance.Retry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.quarkiverse.jms.JmsListener;
import io.quarkus.test.QuarkusExtensionTest;

public class JmsListenerFaultToleranceTest {

    @RegisterExtension
    static final QuarkusExtensionTest config = new QuarkusExtensionTest()
            .overrideConfigKey("quarkus.http.test-port", "0")
            .withApplicationRoot(jar -> jar
                    .addClasses(RetryListener.class, FallbackListener.class,
                            ConnectionFactoryProducer.class, EmbeddedJmsBroker.class)
                    .addAsResource("broker.xml"))
            .setBeforeAllCustomizer(EmbeddedJmsBroker::start)
            .setAfterAllCustomizer(EmbeddedJmsBroker::stop);

    @Inject
    ConnectionFactory connectionFactory;

    @Inject
    RetryListener retryListener;

    @Inject
    FallbackListener fallbackListener;

    @Test
    void retryOnTransientFailure() {
        try (JMSContext ctx = connectionFactory.createContext()) {
            ctx.createProducer().send(ctx.createQueue("ft-retry-queue"), "retry-me");
        }

        await().atMost(10, TimeUnit.SECONDS)
                .until(() -> retryListener.getProcessed().contains("retry-me"));

        assertThat(retryListener.getAttempts().get()).isGreaterThan(1);
    }

    @Test
    void fallbackOnPersistentFailure() {
        try (JMSContext ctx = connectionFactory.createContext()) {
            ctx.createProducer().send(ctx.createQueue("ft-fallback-queue"), "fall-me");
        }

        await().atMost(10, TimeUnit.SECONDS)
                .until(() -> fallbackListener.getFallbacks().contains("fall-me"));

        assertThat(fallbackListener.getProcessed()).isEmpty();
    }

    @ApplicationScoped
    public static class RetryListener {

        private final AtomicInteger attempts = new AtomicInteger();
        private final List<String> processed = new CopyOnWriteArrayList<>();

        @JmsListener("ft-retry-queue")
        @Retry(maxRetries = 3, delay = 100)
        public void onMessage(String message) {
            if (attempts.incrementAndGet() < 3) {
                throw new RuntimeException("transient failure");
            }
            processed.add(message);
        }

        public AtomicInteger getAttempts() {
            return attempts;
        }

        public List<String> getProcessed() {
            return processed;
        }
    }

    @ApplicationScoped
    public static class FallbackListener {

        private final List<String> processed = new CopyOnWriteArrayList<>();
        private final List<String> fallbacks = new CopyOnWriteArrayList<>();

        @JmsListener("ft-fallback-queue")
        @Retry(maxRetries = 2, delay = 100)
        @Fallback(fallbackMethod = "onFallback")
        public void onMessage(String message) {
            throw new RuntimeException("always fails");
        }

        void onFallback(String message) {
            fallbacks.add(message);
        }

        public List<String> getProcessed() {
            return processed;
        }

        public List<String> getFallbacks() {
            return fallbacks;
        }
    }
}
