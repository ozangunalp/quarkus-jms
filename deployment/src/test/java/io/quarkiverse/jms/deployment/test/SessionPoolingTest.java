package io.quarkiverse.jms.deployment.test;

import static org.assertj.core.api.Assertions.assertThat;

import jakarta.inject.Inject;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.JMSConsumer;
import jakarta.jms.JMSContext;
import jakarta.jms.Queue;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.quarkiverse.jms.JmsClient;
import io.quarkus.test.QuarkusExtensionTest;

public class SessionPoolingTest {

    @RegisterExtension
    static final QuarkusExtensionTest config = new QuarkusExtensionTest()
            .overrideConfigKey("quarkus.http.test-port", "0")
            .withApplicationRoot(jar -> jar
                    .addClasses(ConnectionFactoryProducer.class, EmbeddedJmsBroker.class)
                    .addAsResource("broker.xml"))
            .overrideConfigKey("quarkus.jms.pool.enabled", "true")
            .overrideConfigKey("quarkus.jms.pool.max-sessions", "2")
            .overrideConfigKey("quarkus.jms.pool.cache-consumers", "true")
            .setBeforeAllCustomizer(EmbeddedJmsBroker::start)
            .setAfterAllCustomizer(EmbeddedJmsBroker::stop);

    @Inject
    JmsClient jmsClient;

    @Inject
    ConnectionFactory connectionFactory;

    @Test
    void sendAndReceiveWorks() {
        jmsClient.queue("pool-test").send("hello");
        String received = jmsClient.queue("pool-test").receive(String.class).orElse(null);
        assertThat(received).isEqualTo("hello");
    }

    @Test
    void multipleOperationsReusePooledSessions() {
        for (int i = 0; i < 5; i++) {
            jmsClient.queue("pool-reuse").send("msg-" + i);
        }
        for (int i = 0; i < 5; i++) {
            String received = jmsClient.queue("pool-reuse").receive(String.class).orElse(null);
            assertThat(received).isEqualTo("msg-" + i);
        }
    }

    // --- cached consumers ---

    @Test
    void cachedConsumersWorkAcrossPoolRoundTrips() {
        try (JMSContext ctx = connectionFactory.createContext()) {
            Queue queue = ctx.createQueue("cached-consumer-test");
            ctx.createProducer().send(queue, "msg1");
            assertThat(ctx.createConsumer(queue).receive(1000)).isNotNull();
        }

        try (JMSContext ctx = connectionFactory.createContext()) {
            Queue queue = ctx.createQueue("cached-consumer-test");
            ctx.createProducer().send(queue, "msg2");
            assertThat(ctx.createConsumer(queue).receive(1000)).isNotNull();
        }
    }

    @Test
    void differentDestinationsGetDifferentConsumers() {
        JMSConsumer consumerA;
        JMSConsumer consumerB;

        try (JMSContext ctx = connectionFactory.createContext()) {
            consumerA = ctx.createConsumer(ctx.createQueue("dest-a"));
            consumerB = ctx.createConsumer(ctx.createQueue("dest-b"));
        }

        assertThat(consumerA).isNotSameAs(consumerB);
    }
}
