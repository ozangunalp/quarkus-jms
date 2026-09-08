package io.quarkiverse.jms.deployment.test;

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
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.quarkiverse.jms.JmsListener;
import io.quarkus.arc.Unremovable;
import io.quarkus.test.QuarkusExtensionTest;
import io.smallrye.common.annotation.Identifier;

public class JmsListenerNamedConnectionFactoryTest {

    @RegisterExtension
    static final QuarkusExtensionTest config = new QuarkusExtensionTest()
            .withApplicationRoot(jar -> jar
                    .addClasses(NamedCfProducer.class, NamedCfListener.class,
                            EmbeddedJmsBroker.class)
                    .addAsResource("broker.xml"))
            .setBeforeAllCustomizer(EmbeddedJmsBroker::start)
            .setAfterAllCustomizer(EmbeddedJmsBroker::stop);

    @Inject
    @Identifier("broker-b")
    ConnectionFactory namedCf;

    @Inject
    NamedCfListener listener;

    @Test
    void listenerUsesNamedConnectionFactory() {
        try (JMSContext ctx = namedCf.createContext()) {
            ctx.createProducer().send(ctx.createQueue("named-cf-queue"), "hello-named");
        }

        await().atMost(10, TimeUnit.SECONDS)
                .untilAsserted(() -> Assertions.assertThat(listener.getReceived())
                        .contains("hello-named"));
    }

    @ApplicationScoped
    public static class NamedCfProducer {

        @Produces
        @ApplicationScoped
        ConnectionFactory defaultCf() {
            return new ActiveMQConnectionFactory("tcp://localhost:61616");
        }

        @Produces
        @ApplicationScoped
        @Identifier("broker-b")
        @Unremovable
        ConnectionFactory brokerB() {
            return new ActiveMQConnectionFactory("tcp://localhost:61616");
        }
    }

    @ApplicationScoped
    public static class NamedCfListener {

        private final List<String> received = new CopyOnWriteArrayList<>();

        @JmsListener(value = "named-cf-queue", connectionFactory = "broker-b")
        public void onMessage(String message) {
            received.add(message);
        }

        public List<String> getReceived() {
            return received;
        }
    }
}
