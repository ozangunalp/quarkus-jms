package io.quarkiverse.jms.deployment.test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.JMSContext;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.quarkiverse.jms.JmsListenerRegistry;
import io.quarkiverse.jms.JmsListenerRegistry.ListenerConfig;
import io.quarkus.runtime.StartupEvent;
import io.quarkus.test.QuarkusExtensionTest;

public class JmsListenerRegistryVirtualThreadTest {

    @RegisterExtension
    static final QuarkusExtensionTest config = new QuarkusExtensionTest()
            .overrideConfigKey("quarkus.http.test-port", "0")
            .withApplicationRoot(jar -> jar
                    .addClasses(VtListenerSetup.class, ConnectionFactoryProducer.class,
                            EmbeddedJmsBroker.class)
                    .addAsResource("broker.xml"))
            .setBeforeAllCustomizer(EmbeddedJmsBroker::start)
            .setAfterAllCustomizer(EmbeddedJmsBroker::stop);

    @Inject
    ConnectionFactory connectionFactory;

    @Inject
    VtListenerSetup setup;

    @Test
    void programmaticListenerRunsOnVirtualThread() {
        try (JMSContext ctx = connectionFactory.createContext()) {
            ctx.createProducer().send(ctx.createQueue("vt-programmatic-queue"), "vt-msg");
        }

        await().atMost(10, TimeUnit.SECONDS)
                .until(() -> !setup.getReceived().isEmpty());

        assertThat(setup.getReceived()).containsExactly("vt-msg");
        assertThat(setup.getThreadNames().get(0)).contains("quarkus-virtual-thread");
    }

    @ApplicationScoped
    public static class VtListenerSetup {

        private final List<String> received = new CopyOnWriteArrayList<>();
        private final List<String> threadNames = new CopyOnWriteArrayList<>();

        void onStart(@Observes StartupEvent ev, JmsListenerRegistry registry) {
            registry.register(
                    ListenerConfig.queue("vt-programmatic-queue").runOnVirtualThread(),
                    String.class,
                    msg -> {
                        received.add(msg);
                        threadNames.add(Thread.currentThread().getName());
                    });
        }

        public List<String> getReceived() {
            return received;
        }

        public List<String> getThreadNames() {
            return threadNames;
        }
    }
}
