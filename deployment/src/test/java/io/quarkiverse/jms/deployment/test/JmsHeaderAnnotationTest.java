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
import jakarta.jms.TextMessage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.quarkiverse.jms.JmsHeader;
import io.quarkiverse.jms.JmsListener;
import io.quarkus.test.QuarkusExtensionTest;

public class JmsHeaderAnnotationTest {

    @RegisterExtension
    static final QuarkusExtensionTest config = new QuarkusExtensionTest()
            .withApplicationRoot(jar -> jar
                    .addClasses(HeaderListener.class, ConnectionFactoryProducer.class,
                            EmbeddedJmsBroker.class)
                    .addAsResource("broker.xml"))
            .setBeforeAllCustomizer(EmbeddedJmsBroker::start)
            .setAfterAllCustomizer(EmbeddedJmsBroker::stop);

    @Inject
    ConnectionFactory connectionFactory;

    @Inject
    HeaderListener listener;

    @Test
    void jmsHeaderInjectedIntoMethodParameter() throws Exception {
        try (JMSContext context = connectionFactory.createContext()) {
            TextMessage msg = context.createTextMessage("header-payload");
            msg.setStringProperty("source", "test-system");
            context.createProducer().send(context.createQueue("header-queue"), msg);
        }

        await().atMost(10, TimeUnit.SECONDS)
                .until(() -> !listener.getRecords().isEmpty());

        assertThat(listener.getRecords().get(0)).isEqualTo("header-payload|test-system");
    }

    @ApplicationScoped
    public static class HeaderListener {

        private final CopyOnWriteArrayList<String> records = new CopyOnWriteArrayList<>();

        @JmsListener("header-queue")
        public void onMessage(String payload, @JmsHeader("source") String source) {
            records.add(payload + "|" + source);
        }

        public List<String> getRecords() {
            return records;
        }
    }
}
