package io.quarkiverse.jms.deployment.test;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import jakarta.inject.Inject;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.quarkiverse.jms.JmsClient;
import io.quarkus.test.QuarkusExtensionTest;

public class JmsPoolMetricsTest {

    @RegisterExtension
    static final QuarkusExtensionTest config = new QuarkusExtensionTest()
            .withApplicationRoot(jar -> jar
                    .addClasses(ConnectionFactoryProducer.class, EmbeddedJmsBroker.class)
                    .addAsResource("broker.xml"))
            .overrideConfigKey("quarkus.jms.pool.max-sessions", "5")
            .overrideConfigKey("quarkus.jms.pool.borrow-timeout", "PT2S")
            .setBeforeAllCustomizer(EmbeddedJmsBroker::start)
            .setAfterAllCustomizer(EmbeddedJmsBroker::stop);

    @Inject
    JmsClient jmsClient;

    @Inject
    MeterRegistry registry;

    @Test
    void poolGaugesRegisteredAfterFirstOperation() {
        jmsClient.queue("pool-metrics-test").send("trigger");

        Gauge active = gauge("jms.pool.active");
        Gauge idle = gauge("jms.pool.idle");
        Gauge borrows = gauge("jms.pool.borrow.total");
        Gauge timeouts = gauge("jms.pool.timeout.total");

        assertThat(active).isNotNull();
        assertThat(idle).isNotNull();
        assertThat(borrows).isNotNull();
        assertThat(timeouts).isNotNull();

        assertThat(borrows.value()).isGreaterThanOrEqualTo(1.0);
        assertThat(timeouts.value()).isEqualTo(0.0);
    }

    @Test
    void activeIncrementsWhileSessionHeld() throws Exception {
        jmsClient.queue("metrics-active-test").send("setup");

        double activeBefore = gauge("jms.pool.active").value();
        double borrowsBefore = gauge("jms.pool.borrow.total").value();

        CountDownLatch holdLatch = new CountDownLatch(1);
        CountDownLatch readyLatch = new CountDownLatch(1);
        ExecutorService executor = Executors.newSingleThreadExecutor();

        executor.submit(() -> {
            jmsClient.queue("metrics-active-test").execute(ctx -> {
                readyLatch.countDown();
                try {
                    holdLatch.await(10, TimeUnit.SECONDS);
                } catch (InterruptedException ignored) {
                }
                return null;
            });
        });

        assertThat(readyLatch.await(5, TimeUnit.SECONDS)).isTrue();

        assertThat(gauge("jms.pool.active").value()).isGreaterThan(activeBefore);
        assertThat(gauge("jms.pool.borrow.total").value()).isGreaterThan(borrowsBefore);

        holdLatch.countDown();
        executor.shutdown();
        assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();

        assertThat(gauge("jms.pool.active").value()).isEqualTo(activeBefore);
    }

    @Test
    void idleReflectsReturnedSessions() {
        double idleBefore = gauge("jms.pool.idle").value();

        jmsClient.queue("metrics-idle-test").send("create-session");

        assertThat(gauge("jms.pool.idle").value()).isGreaterThanOrEqualTo(idleBefore);
    }

    private Gauge gauge(String name) {
        return registry.find(name).tag("factory", "__default__").gauge();
    }
}
