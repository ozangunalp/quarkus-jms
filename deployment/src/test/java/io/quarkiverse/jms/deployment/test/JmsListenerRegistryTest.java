package io.quarkiverse.jms.deployment.test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.io.Closeable;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.JMSContext;
import jakarta.jms.JMSException;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.quarkiverse.jms.JmsListenerRegistry;
import io.quarkiverse.jms.JmsListenerRegistry.ListenerConfig;
import io.quarkus.runtime.StartupEvent;
import io.quarkus.test.QuarkusExtensionTest;

public class JmsListenerRegistryTest {

    @RegisterExtension
    static final QuarkusExtensionTest config = new QuarkusExtensionTest()
            .overrideConfigKey("quarkus.http.test-port", "0")
            .withApplicationRoot(jar -> jar
                    .addClasses(ProgrammaticListenerSetup.class, ConnectionFactoryProducer.class,
                            EmbeddedJmsBroker.class)
                    .addAsResource("broker.xml"))
            .setBeforeAllCustomizer(EmbeddedJmsBroker::start)
            .setAfterAllCustomizer(EmbeddedJmsBroker::stop);

    @Inject
    ConnectionFactory connectionFactory;

    @Inject
    JmsListenerRegistry registry;

    @Inject
    ProgrammaticListenerSetup setup;

    @Test
    void programmaticRawListenerReceivesMessages() throws Exception {
        Thread.sleep(500);

        try (JMSContext context = connectionFactory.createContext()) {
            context.createProducer().send(context.createQueue("programmatic-queue"), "raw-hello");
        }

        await().atMost(10, TimeUnit.SECONDS)
                .until(() -> setup.getRawMessages().contains("raw-hello"));
    }

    @Test
    void programmaticTypedListenerReceivesMessages() throws Exception {
        Thread.sleep(500);

        try (JMSContext context = connectionFactory.createContext()) {
            context.createProducer().send(context.createQueue("typed-queue"), "typed-hello");
        }

        await().atMost(10, TimeUnit.SECONDS)
                .until(() -> setup.getTypedMessages().contains("typed-hello"));
    }

    @Test
    void programmaticListenerWithSelectorFiltersMessages() throws Exception {
        Thread.sleep(500);

        try (JMSContext context = connectionFactory.createContext()) {
            context.createProducer()
                    .setProperty("priority", "HIGH")
                    .send(context.createQueue("selector-queue"), "high-msg");
            context.createProducer()
                    .setProperty("priority", "LOW")
                    .send(context.createQueue("selector-queue"), "low-msg");
        }

        await().atMost(10, TimeUnit.SECONDS)
                .until(() -> setup.getSelectorMessages().contains("high-msg"));

        Thread.sleep(500);
        Assertions.assertThat(setup.getSelectorMessages()).doesNotContain("low-msg");
    }

    // --- closeable handle ---

    @Test
    void closingHandleStopsListener() throws Exception {
        List<String> received = new CopyOnWriteArrayList<>();

        Closeable handle = registry.register("closeable-queue", String.class, received::add);

        Thread.sleep(500);

        try (JMSContext ctx = connectionFactory.createContext()) {
            ctx.createProducer().send(ctx.createQueue("closeable-queue"), "before-close");
        }

        await().atMost(10, TimeUnit.SECONDS)
                .until(() -> received.contains("before-close"));

        handle.close();

        Thread.sleep(500);

        try (JMSContext ctx = connectionFactory.createContext()) {
            ctx.createProducer().send(ctx.createQueue("closeable-queue"), "after-close");
        }

        Thread.sleep(2000);

        assertThat(received).containsExactly("before-close");
    }

    @ApplicationScoped
    public static class ProgrammaticListenerSetup {

        private final List<String> rawMessages = new CopyOnWriteArrayList<>();
        private final List<String> typedMessages = new CopyOnWriteArrayList<>();
        private final List<String> selectorMessages = new CopyOnWriteArrayList<>();

        void onStart(@Observes StartupEvent ev, JmsListenerRegistry registry) {
            registry.register("programmatic-queue", msg -> {
                try {
                    rawMessages.add(msg.getBody(String.class));
                } catch (JMSException e) {
                    throw new RuntimeException(e);
                }
            });

            registry.register("typed-queue", String.class, typedMessages::add);

            registry.register(
                    ListenerConfig.queue("selector-queue").selector("priority = 'HIGH'"),
                    String.class,
                    selectorMessages::add);
        }

        public List<String> getRawMessages() {
            return rawMessages;
        }

        public List<String> getTypedMessages() {
            return typedMessages;
        }

        public List<String> getSelectorMessages() {
            return selectorMessages;
        }
    }
}
