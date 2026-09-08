package io.quarkiverse.messaging.jms.deployment.test;

import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.quarkus.test.QuarkusExtensionTest;
import io.smallrye.mutiny.Multi;

public class QuarkusJmsConnectorRoundTripTest {

    @RegisterExtension
    static final QuarkusExtensionTest config = new QuarkusExtensionTest()
            .withApplicationRoot(jar -> jar
                    .addClasses(Producer.class, Consumer.class, ConnectionFactoryProducer.class,
                            EmbeddedJmsBroker.class)
                    .addAsResource("broker.xml"))
            .overrideConfigKey("mp.messaging.outgoing.to-jms.connector", "smallrye-jms")
            .overrideConfigKey("mp.messaging.outgoing.to-jms.destination", "roundtrip-queue")
            .overrideConfigKey("mp.messaging.incoming.from-jms.connector", "smallrye-jms")
            .overrideConfigKey("mp.messaging.incoming.from-jms.destination", "roundtrip-queue")
            .setBeforeAllCustomizer(EmbeddedJmsBroker::start)
            .setAfterAllCustomizer(EmbeddedJmsBroker::stop);

    @Inject
    Consumer consumer;

    @Test
    void outgoingToIncomingRoundTrip() {
        await().atMost(30, TimeUnit.SECONDS)
                .until(() -> consumer.getReceived().size() >= 3);
    }

    @ApplicationScoped
    public static class Producer {

        @Outgoing("to-jms")
        public Multi<String> produce() {
            return Multi.createFrom().ticks().every(Duration.ofMillis(500))
                    .map(tick -> "round-" + tick);
        }
    }

    @ApplicationScoped
    public static class Consumer {

        private final CopyOnWriteArrayList<String> received = new CopyOnWriteArrayList<>();

        @Incoming("from-jms")
        public void consume(String payload) {
            received.add(payload);
        }

        public List<String> getReceived() {
            return received;
        }
    }
}
