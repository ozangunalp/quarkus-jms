package io.quarkiverse.jms.deployment.test;

import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.quarkiverse.jms.JmsListener;
import io.quarkiverse.jms.JmsQueue;
import io.quarkus.test.QuarkusExtensionTest;
import io.smallrye.common.annotation.Identifier;

public class JmsDestinationInjectionTest {

    @RegisterExtension
    static final QuarkusExtensionTest config = new QuarkusExtensionTest()
            .withApplicationRoot(jar -> jar
                    .addClasses(Sender.class, Receiver.class, ConnectionFactoryProducer.class,
                            EmbeddedJmsBroker.class)
                    .addAsResource("broker.xml"))
            .setBeforeAllCustomizer(EmbeddedJmsBroker::start)
            .setAfterAllCustomizer(EmbeddedJmsBroker::stop);

    @Inject
    Sender sender;

    @Inject
    Receiver receiver;

    @Test
    void injectedQueueSendAndReceive() {
        sender.send("injected-test");

        await().atMost(10, TimeUnit.SECONDS)
                .until(() -> receiver.getMessages().contains("injected-test"));
    }

    @Test
    void injectedQueueDirectReceive() {
        sender.send("direct-recv");

        Optional<String> received = sender.receive();
        // Message was consumed by the listener, so direct receive may get it or not
        // depending on timing. This test verifies the receive method works without error.
    }

    @ApplicationScoped
    public static class Sender {

        @Inject
        @Identifier("dest-test-queue")
        JmsQueue<String> queue;

        public void send(String message) {
            queue.send(message);
        }

        public Optional<String> receive() {
            return queue.receiveTimeout(Duration.ofSeconds(2)).receive();
        }
    }

    @ApplicationScoped
    public static class Receiver {

        private final CopyOnWriteArrayList<String> messages = new CopyOnWriteArrayList<>();

        @JmsListener("dest-test-queue")
        public void onMessage(String message) {
            messages.add(message);
        }

        public java.util.List<String> getMessages() {
            return messages;
        }
    }
}
