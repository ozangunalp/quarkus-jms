package io.quarkiverse.jms.deployment.test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.JMSContext;
import jakarta.jms.JMSException;
import jakarta.jms.Message;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.quarkiverse.jms.JmsListener;
import io.quarkus.test.QuarkusExtensionTest;

public class JmsListenerClientAcknowledgeTest {

    @RegisterExtension
    static final QuarkusExtensionTest config = new QuarkusExtensionTest()
            .withApplicationRoot(jar -> jar
                    .addClasses(ClientAckListener.class, ConnectionFactoryProducer.class,
                            EmbeddedJmsBroker.class)
                    .addAsResource("broker.xml"))
            .setBeforeAllCustomizer(EmbeddedJmsBroker::start)
            .setAfterAllCustomizer(EmbeddedJmsBroker::stop);

    @Inject
    ConnectionFactory connectionFactory;

    @Inject
    ClientAckListener listener;

    @Test
    void clientAcknowledgeModeReceivesMessages() {
        try (JMSContext ctx = connectionFactory.createContext()) {
            ctx.createProducer().send(ctx.createQueue("client-ack-queue"), "ack-me");
        }

        await().atMost(10, TimeUnit.SECONDS)
                .until(() -> !listener.getReceived().isEmpty());

        assertThat(listener.getReceived()).containsExactly("ack-me");
    }

    @ApplicationScoped
    public static class ClientAckListener {

        private final List<String> received = new CopyOnWriteArrayList<>();

        @JmsListener(value = "client-ack-queue", acknowledge = JmsListener.AcknowledgeMode.CLIENT)
        public void onMessage(Message message) throws JMSException {
            received.add(message.getBody(String.class));
            message.acknowledge();
        }

        public List<String> getReceived() {
            return received;
        }
    }
}
