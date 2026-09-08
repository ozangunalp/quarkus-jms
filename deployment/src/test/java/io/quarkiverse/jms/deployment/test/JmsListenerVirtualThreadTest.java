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

import io.quarkiverse.jms.JmsListener;
import io.quarkus.test.QuarkusExtensionTest;
import io.smallrye.common.annotation.RunOnVirtualThread;

public class JmsListenerVirtualThreadTest {

    @RegisterExtension
    static final QuarkusExtensionTest config = new QuarkusExtensionTest()
            .withApplicationRoot(jar -> jar
                    .addClasses(VirtualThreadListener.class, ConnectionFactoryProducer.class,
                            EmbeddedJmsBroker.class)
                    .addAsResource("broker.xml"))
            .setBeforeAllCustomizer(EmbeddedJmsBroker::start)
            .setAfterAllCustomizer(EmbeddedJmsBroker::stop);

    @Inject
    ConnectionFactory connectionFactory;

    @Inject
    VirtualThreadListener listener;

    @Test
    void listenerRunsOnVirtualThread() {
        try (JMSContext ctx = connectionFactory.createContext()) {
            ctx.createProducer().send(ctx.createQueue("vt-queue"), "vt-msg");
        }

        await().atMost(10, TimeUnit.SECONDS)
                .until(() -> !listener.getThreadNames().isEmpty());

        assertThat(listener.getReceived()).containsExactly("vt-msg");
        assertThat(listener.getThreadNames().get(0)).contains("quarkus-virtual-thread");
    }

    @ApplicationScoped
    public static class VirtualThreadListener {

        private final List<String> received = new CopyOnWriteArrayList<>();
        private final List<String> threadNames = new CopyOnWriteArrayList<>();

        @JmsListener("vt-queue")
        @RunOnVirtualThread
        public void onMessage(String message) {
            received.add(message);
            threadNames.add(Thread.currentThread().getName());
        }

        public List<String> getReceived() {
            return received;
        }

        public List<String> getThreadNames() {
            return threadNames;
        }
    }
}
