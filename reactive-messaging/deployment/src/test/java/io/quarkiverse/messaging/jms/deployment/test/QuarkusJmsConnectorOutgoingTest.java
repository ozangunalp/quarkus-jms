package io.quarkiverse.messaging.jms.deployment.test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.JMSConsumer;
import jakarta.jms.JMSContext;
import jakarta.jms.TextMessage;

import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.quarkus.test.QuarkusExtensionTest;
import io.smallrye.mutiny.Multi;

public class QuarkusJmsConnectorOutgoingTest {

    @RegisterExtension
    static final QuarkusExtensionTest config = new QuarkusExtensionTest()
            .withApplicationRoot(jar -> jar
                    .addClasses(OutgoingProducer.class, ConnectionFactoryProducer.class,
                            EmbeddedJmsBroker.class)
                    .addAsResource("broker.xml"))
            .overrideConfigKey("mp.messaging.outgoing.data-out.connector", "smallrye-jms")
            .overrideConfigKey("mp.messaging.outgoing.data-out.destination", "rm-outgoing-queue")
            .setBeforeAllCustomizer(EmbeddedJmsBroker::start)
            .setAfterAllCustomizer(EmbeddedJmsBroker::stop);

    @Inject
    ConnectionFactory connectionFactory;

    @Test
    void outgoingChannelSendsMessages() {
        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            try (JMSContext context = connectionFactory.createContext()) {
                JMSConsumer consumer = context.createConsumer(context.createQueue("rm-outgoing-queue"));
                TextMessage msg = (TextMessage) consumer.receive(2000);
                assertThat(msg).isNotNull();
                assertThat(msg.getText()).startsWith("tick-");
            }
        });
    }

    @ApplicationScoped
    public static class OutgoingProducer {

        @Outgoing("data-out")
        public Multi<String> produce() {
            return Multi.createFrom().ticks().every(Duration.ofMillis(500))
                    .map(tick -> "tick-" + tick);
        }
    }
}
