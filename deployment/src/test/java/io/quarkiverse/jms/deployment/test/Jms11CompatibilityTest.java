package io.quarkiverse.jms.deployment.test;

import static org.assertj.core.api.Assertions.assertThat;

import jakarta.inject.Inject;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.JMSContext;
import jakarta.jms.Queue;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.quarkiverse.jms.JmsClient;
import io.quarkus.test.QuarkusExtensionTest;

public class Jms11CompatibilityTest {

    @RegisterExtension
    static final QuarkusExtensionTest config = new QuarkusExtensionTest()
            .withApplicationRoot(jar -> jar
                    .addClasses(ConnectionFactoryProducer.class, EmbeddedJmsBroker.class)
                    .addAsResource("broker.xml"))
            .overrideConfigKey("quarkus.jms.jms11", "true")
            .overrideConfigKey("quarkus.jms.pool.enabled", "true")
            .overrideConfigKey("quarkus.jms.pool.max-sessions", "2")
            .setBeforeAllCustomizer(EmbeddedJmsBroker::start)
            .setAfterAllCustomizer(EmbeddedJmsBroker::stop);

    @Inject
    JmsClient jmsClient;

    @Inject
    ConnectionFactory connectionFactory;

    @Test
    void sendAndReceiveInJms11Mode() {
        jmsClient.queue("jms11-test").send("hello-jms11");
        String received = jmsClient.queue("jms11-test").receive(String.class).orElse(null);
        assertThat(received).isEqualTo("hello-jms11");
    }

    @Test
    void contextWrapsConnectionAndSession() {
        try (JMSContext ctx = connectionFactory.createContext()) {
            // The PooledJMSContext delegates to a JmsContextWrapper in JMS 1.1 mode
            Queue queue = ctx.createQueue("jms11-ctx-test");
            ctx.createProducer().send(queue, "test");
            assertThat(ctx.createConsumer(queue).receive(1000)).isNotNull();
        }
    }

    @Test
    void multipleOperationsWorkInJms11Mode() {
        for (int i = 0; i < 5; i++) {
            jmsClient.queue("jms11-multi").send("msg-" + i);
        }
        for (int i = 0; i < 5; i++) {
            String received = jmsClient.queue("jms11-multi").receive(String.class).orElse(null);
            assertThat(received).isEqualTo("msg-" + i);
        }
    }
}
