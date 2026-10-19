package io.quarkiverse.jms.deployment.test;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import jakarta.inject.Inject;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.quarkiverse.jms.JmsClient;
import io.quarkus.test.QuarkusExtensionTest;

public class SessionPoolConcurrencyTest {

    @RegisterExtension
    static final QuarkusExtensionTest config = new QuarkusExtensionTest()
            .overrideConfigKey("quarkus.http.test-port", "0")
            .withApplicationRoot(jar -> jar
                    .addClasses(ConnectionFactoryProducer.class, EmbeddedJmsBroker.class)
                    .addAsResource("broker.xml"))
            .overrideConfigKey("quarkus.jms.pool.enabled", "true")
            .overrideConfigKey("quarkus.jms.pool.max-sessions", "5")
            .overrideConfigKey("quarkus.jms.pool.borrow-timeout", "PT2S")
            .overrideConfigKey("quarkus.jms.pool.idle-timeout", "PT1S")
            .setBeforeAllCustomizer(EmbeddedJmsBroker::start)
            .setAfterAllCustomizer(EmbeddedJmsBroker::stop);

    @Inject
    JmsClient jmsClient;

    @Test
    void concurrentSendsSucceedWithBoundedPool() throws Exception {
        int threads = 20;
        int messagesPerThread = 10;
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        CountDownLatch startLatch = new CountDownLatch(1);
        List<Throwable> errors = Collections.synchronizedList(new ArrayList<>());

        List<Future<?>> futures = new ArrayList<>();
        for (int t = 0; t < threads; t++) {
            int threadId = t;
            futures.add(executor.submit(() -> {
                try {
                    startLatch.await();
                    for (int i = 0; i < messagesPerThread; i++) {
                        jmsClient.queue("concurrent-pool-test").send("t" + threadId + "-m" + i);
                    }
                } catch (Throwable e) {
                    errors.add(e);
                }
            }));
        }

        startLatch.countDown();
        for (Future<?> f : futures) {
            f.get(30, TimeUnit.SECONDS);
        }
        executor.shutdown();

        assertThat(errors).isEmpty();

        int received = 0;
        while (jmsClient.queue("concurrent-pool-test").receive(String.class).isPresent()) {
            received++;
        }
        assertThat(received).isEqualTo(threads * messagesPerThread);
    }

    @Test
    void idleSessionsAreEvicted() throws Exception {
        for (int i = 0; i < 3; i++) {
            jmsClient.queue("eviction-test").send("msg-" + i);
        }

        Thread.sleep(2000);

        jmsClient.queue("eviction-test").send("after-eviction");

        int received = 0;
        while (jmsClient.queue("eviction-test").receive(String.class).isPresent()) {
            received++;
        }
        assertThat(received).isEqualTo(4);
    }
}
