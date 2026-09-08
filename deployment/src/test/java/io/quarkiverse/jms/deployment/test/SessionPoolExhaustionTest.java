package io.quarkiverse.jms.deployment.test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import jakarta.inject.Inject;
import jakarta.jms.JMSRuntimeException;

import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.quarkiverse.jms.JmsClient;
import io.quarkiverse.jms.JmsSendException;
import io.quarkus.test.QuarkusExtensionTest;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class SessionPoolExhaustionTest {

    @RegisterExtension
    static final QuarkusExtensionTest config = new QuarkusExtensionTest()
            .withApplicationRoot(jar -> jar
                    .addClasses(ConnectionFactoryProducer.class, EmbeddedJmsBroker.class)
                    .addAsResource("broker.xml"))
            .overrideConfigKey("quarkus.jms.pool.enabled", "true")
            .overrideConfigKey("quarkus.jms.pool.max-sessions", "5")
            .overrideConfigKey("quarkus.jms.pool.borrow-timeout", "PT2S")
            .setBeforeAllCustomizer(EmbeddedJmsBroker::start)
            .setAfterAllCustomizer(EmbeddedJmsBroker::stop);

    @Inject
    JmsClient jmsClient;

    @Inject
    MeterRegistry registry;

    @Test
    @Order(1)
    void poolExhaustionThrowsOnTimeout() throws Exception {
        CountDownLatch holdLatch = new CountDownLatch(1);
        CountDownLatch readyLatch = new CountDownLatch(5);
        ExecutorService executor = Executors.newFixedThreadPool(6);

        for (int i = 0; i < 5; i++) {
            executor.submit(() -> {
                jmsClient.queue("exhaust-test").execute(ctx -> {
                    readyLatch.countDown();
                    try {
                        holdLatch.await(30, TimeUnit.SECONDS);
                    } catch (InterruptedException ignored) {
                    }
                    return null;
                });
            });
        }

        assertThat(readyLatch.await(10, TimeUnit.SECONDS)).isTrue();

        assertThatThrownBy(() -> jmsClient.queue("exhaust-test").send("should-fail"))
                .isInstanceOf(JmsSendException.class)
                .hasCauseInstanceOf(JMSRuntimeException.class);

        holdLatch.countDown();
        executor.shutdown();
        assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
    }

    @Test
    @Order(2)
    void timeoutMetricIncrements() throws Exception {
        jmsClient.queue("metrics-trigger").send("trigger");

        Gauge timeouts = registry.find("jms.pool.timeout.total").tag("factory", "__default__").gauge();
        assertThat(timeouts).isNotNull();
        double timeoutsBefore = timeouts.value();

        CountDownLatch holdLatch = new CountDownLatch(1);
        CountDownLatch readyLatch = new CountDownLatch(5);
        ExecutorService executor = Executors.newFixedThreadPool(6);
        List<Future<?>> futures = new ArrayList<>();

        for (int i = 0; i < 5; i++) {
            futures.add(executor.submit(() -> {
                jmsClient.queue("metrics-timeout-test").execute(ctx -> {
                    readyLatch.countDown();
                    try {
                        holdLatch.await(30, TimeUnit.SECONDS);
                    } catch (InterruptedException ignored) {
                    }
                    return null;
                });
            }));
        }

        assertThat(readyLatch.await(10, TimeUnit.SECONDS)).isTrue();

        try {
            jmsClient.queue("metrics-timeout-test").send("should-timeout");
        } catch (Exception expected) {
        }

        assertThat(timeouts.value()).isGreaterThan(timeoutsBefore);

        holdLatch.countDown();
        executor.shutdown();
        assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
    }
}
