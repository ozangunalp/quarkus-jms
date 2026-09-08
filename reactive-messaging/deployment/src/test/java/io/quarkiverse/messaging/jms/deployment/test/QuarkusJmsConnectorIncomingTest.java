package io.quarkiverse.messaging.jms.deployment.test;

import static org.awaitility.Awaitility.await;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.JMSContext;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.quarkus.test.QuarkusExtensionTest;

public class QuarkusJmsConnectorIncomingTest {

    @RegisterExtension
    static final QuarkusExtensionTest config = new QuarkusExtensionTest()
            .withApplicationRoot(jar -> jar
                    .addClasses(IncomingConsumer.class, ConnectionFactoryProducer.class,
                            EmbeddedJmsBroker.class)
                    .addAsResource("broker.xml"))
            .overrideConfigKey("mp.messaging.incoming.data.connector", "smallrye-jms")
            .overrideConfigKey("mp.messaging.incoming.data.destination", "rm-incoming-queue")
            .setBeforeAllCustomizer(EmbeddedJmsBroker::start)
            .setAfterAllCustomizer(EmbeddedJmsBroker::stop);

    @Inject
    ConnectionFactory connectionFactory;

    @Inject
    IncomingConsumer consumer;

    @Test
    void incomingChannelReceivesMessages() {
        try (JMSContext context = connectionFactory.createContext()) {
            context.createProducer().send(context.createQueue("rm-incoming-queue"), "rm-hello");
        }

        await().atMost(10, TimeUnit.SECONDS)
                .until(() -> consumer.getMessages().contains("rm-hello"));
    }

    @ApplicationScoped
    public static class IncomingConsumer {

        private final CopyOnWriteArrayList<String> messages = new CopyOnWriteArrayList<>();

        @Incoming("data")
        public void consume(String payload) {
            messages.add(payload);
        }

        public List<String> getMessages() {
            return messages;
        }
    }
}
