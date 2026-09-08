package io.quarkiverse.jms.deployment.test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Alternative;
import jakarta.inject.Inject;
import jakarta.jms.JMSContext;
import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.TextMessage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.quarkiverse.jms.JmsClient;
import io.quarkiverse.jms.JmsListener;
import io.quarkiverse.jms.JmsMessageConverter;
import io.quarkus.test.QuarkusExtensionTest;

public class JmsCustomConverterTest {

    @RegisterExtension
    static final QuarkusExtensionTest config = new QuarkusExtensionTest()
            .withApplicationRoot(jar -> jar
                    .addClasses(CsvConverter.class, Receiver.class,
                            ConnectionFactoryProducer.class, EmbeddedJmsBroker.class)
                    .addAsResource("broker.xml"))
            .setBeforeAllCustomizer(EmbeddedJmsBroker::start)
            .setAfterAllCustomizer(EmbeddedJmsBroker::stop);

    @Inject
    JmsClient jmsClient;

    @Inject
    Receiver receiver;

    @Test
    void customConverterUsedForSend() {
        jmsClient.queue("custom-conv-queue").send(new String[] { "a", "b", "c" });

        Optional<String> raw = jmsClient.queue("custom-conv-queue")
                .receive(String.class);

        assertThat(raw).contains("a,b,c");
    }

    @Test
    void customConverterUsedForListenerDeserialization() {
        jmsClient.queue("custom-conv-listener").send(new String[] { "x", "y" });

        await().atMost(10, TimeUnit.SECONDS)
                .until(() -> receiver.getMessages().contains("x,y"));
    }

    @ApplicationScoped
    @Alternative
    @jakarta.annotation.Priority(1)
    public static class CsvConverter implements JmsMessageConverter {

        @Override
        public boolean supports(Class<?> type) {
            return type == String[].class || type == String.class;
        }

        @Override
        public Message toMessage(Object payload, JMSContext context) {
            if (payload instanceof String[] arr) {
                return context.createTextMessage(String.join(",", arr));
            }
            return context.createTextMessage(payload.toString());
        }

        @Override
        @SuppressWarnings("unchecked")
        public <T> T fromMessage(Message message, Class<T> type) {
            try {
                String text = ((TextMessage) message).getText();
                if (type == String[].class) {
                    return (T) text.split(",");
                }
                return (T) text;
            } catch (JMSException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @ApplicationScoped
    public static class Receiver {

        private final CopyOnWriteArrayList<String> messages = new CopyOnWriteArrayList<>();

        @JmsListener("custom-conv-listener")
        public void onMessage(String message) {
            messages.add(message);
        }

        public List<String> getMessages() {
            return messages;
        }
    }
}
