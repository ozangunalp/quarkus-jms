package io.quarkiverse.jms.deployment.test;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import jakarta.inject.Inject;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.JMSContext;
import jakarta.jms.TextMessage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.quarkiverse.jms.JmsClient;
import io.quarkiverse.jms.ReceiveMessage;
import io.quarkus.test.QuarkusExtensionTest;

public class JmsBrowseMessagesTest {

    @RegisterExtension
    static final QuarkusExtensionTest config = new QuarkusExtensionTest()
            .withApplicationRoot(jar -> jar
                    .addClasses(ConnectionFactoryProducer.class, EmbeddedJmsBroker.class)
                    .addAsResource("broker.xml"))
            .setBeforeAllCustomizer(EmbeddedJmsBroker::start)
            .setAfterAllCustomizer(EmbeddedJmsBroker::stop);

    @Inject
    JmsClient jmsClient;

    @Inject
    ConnectionFactory connectionFactory;

    @Test
    void browseMessagesReturnsPayloadAndRawMessage() throws Exception {
        try (JMSContext ctx = connectionFactory.createContext()) {
            TextMessage msg = ctx.createTextMessage("browse-payload");
            msg.setStringProperty("origin", "test");
            ctx.createProducer().send(ctx.createQueue("browse-msg-queue"), msg);
        }

        List<ReceiveMessage<String>> results = jmsClient.queue("browse-msg-queue", String.class)
                .browseMessages();

        assertThat(results).hasSize(1);
        assertThat(results.get(0).payload()).isEqualTo("browse-payload");
        assertThat(results.get(0).message()).isNotNull();
        assertThat(results.get(0).message().getStringProperty("origin")).isEqualTo("test");
    }

    @Test
    void browseMessagesWithSelector() throws Exception {
        try (JMSContext ctx = connectionFactory.createContext()) {
            TextMessage high = ctx.createTextMessage("high-priority");
            high.setIntProperty("priority", 10);
            ctx.createProducer().send(ctx.createQueue("browse-sel-queue"), high);

            TextMessage low = ctx.createTextMessage("low-priority");
            low.setIntProperty("priority", 1);
            ctx.createProducer().send(ctx.createQueue("browse-sel-queue"), low);
        }

        List<ReceiveMessage<String>> highOnly = jmsClient.queue("browse-sel-queue", String.class)
                .browseMessages("priority > 5");

        assertThat(highOnly).hasSize(1);
        assertThat(highOnly.get(0).payload()).isEqualTo("high-priority");
    }
}
