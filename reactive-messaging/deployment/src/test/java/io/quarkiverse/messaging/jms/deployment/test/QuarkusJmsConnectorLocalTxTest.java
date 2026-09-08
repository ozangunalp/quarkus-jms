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

public class QuarkusJmsConnectorLocalTxTest {

    @RegisterExtension
    static final QuarkusExtensionTest config = new QuarkusExtensionTest()
            .withApplicationRoot(jar -> jar
                    .addClasses(TxConsumer.class, ConnectionFactoryProducer.class,
                            EmbeddedJmsBroker.class)
                    .addAsResource("broker.xml"))
            .overrideConfigKey("mp.messaging.incoming.tx-data.connector", "smallrye-jms")
            .overrideConfigKey("mp.messaging.incoming.tx-data.destination", "local-tx-queue")
            .overrideConfigKey("mp.messaging.incoming.tx-data.transaction-mode", "local")
            .overrideConfigKey("mp.messaging.incoming.tx-data.session-mode", "SESSION_TRANSACTED")
            .setBeforeAllCustomizer(EmbeddedJmsBroker::start)
            .setAfterAllCustomizer(EmbeddedJmsBroker::stop);

    @Inject
    ConnectionFactory connectionFactory;

    @Inject
    TxConsumer consumer;

    @Test
    void localTransactionCommitsOnSuccess() {
        try (JMSContext context = connectionFactory.createContext()) {
            context.createProducer().send(context.createQueue("local-tx-queue"), "tx-msg");
        }

        await().atMost(10, TimeUnit.SECONDS)
                .until(() -> consumer.getReceived().contains("tx-msg"));
    }

    @ApplicationScoped
    public static class TxConsumer {

        private final CopyOnWriteArrayList<String> received = new CopyOnWriteArrayList<>();

        @Incoming("tx-data")
        public void consume(String payload) {
            received.add(payload);
        }

        public List<String> getReceived() {
            return received;
        }
    }
}
