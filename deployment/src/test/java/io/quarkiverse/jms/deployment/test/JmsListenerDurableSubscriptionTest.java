package io.quarkiverse.jms.deployment.test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.JMSContext;

import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.quarkiverse.jms.JmsListener;
import io.quarkus.test.QuarkusExtensionTest;

public class JmsListenerDurableSubscriptionTest {

    @RegisterExtension
    static final QuarkusExtensionTest config = new QuarkusExtensionTest()
            .withApplicationRoot(jar -> jar
                    .addClasses(DurableSubscriber.class, DurableConnectionFactoryProducer.class,
                            EmbeddedJmsBroker.class)
                    .addAsResource("broker.xml"))
            .setBeforeAllCustomizer(EmbeddedJmsBroker::start)
            .setAfterAllCustomizer(EmbeddedJmsBroker::stop);

    @ApplicationScoped
    public static class DurableConnectionFactoryProducer {

        @Produces
        @ApplicationScoped
        ConnectionFactory connectionFactory() {
            ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory("tcp://localhost:61616");
            cf.setClientID("durable-test-client");
            return cf;
        }
    }

    @Inject
    DurableSubscriber subscriber;

    @Test
    void durableSubscriptionReceivesMessages() {
        try (ActiveMQConnectionFactory sendCf = new ActiveMQConnectionFactory("tcp://localhost:61616");
                JMSContext ctx = sendCf.createContext()) {
            ctx.createProducer().send(ctx.createTopic("durable-topic"), "durable-msg");
        }

        await().atMost(10, TimeUnit.SECONDS)
                .until(() -> !subscriber.getReceived().isEmpty());

        assertThat(subscriber.getReceived()).containsExactly("durable-msg");
    }

    @ApplicationScoped
    public static class DurableSubscriber {

        private final List<String> received = new CopyOnWriteArrayList<>();

        @JmsListener(value = "durable-topic", topic = true, subscription = "my-durable-sub")
        public void onMessage(String message) {
            received.add(message);
        }

        public List<String> getReceived() {
            return received;
        }
    }
}
