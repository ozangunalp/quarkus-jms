package io.quarkiverse.jms.deployment.test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.JMSContext;
import jakarta.jms.Topic;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.quarkiverse.jms.JmsListener;
import io.quarkiverse.jms.JmsResponse;
import io.quarkiverse.jms.SendTo;
import io.quarkus.test.QuarkusExtensionTest;

public class JmsResponseRoutingTest {

    @RegisterExtension
    static final QuarkusExtensionTest config = new QuarkusExtensionTest()
            .withApplicationRoot(jar -> jar
                    .addClasses(SendToListener.class, TopicRouter.class,
                            ConnectionFactoryProducer.class, EmbeddedJmsBroker.class)
                    .addAsResource("broker.xml"))
            .setBeforeAllCustomizer(EmbeddedJmsBroker::start)
            .setAfterAllCustomizer(EmbeddedJmsBroker::stop);

    @Inject
    ConnectionFactory connectionFactory;

    // --- @SendTo ---

    @Test
    void returnValueSentToDestination() throws Exception {
        try (JMSContext context = connectionFactory.createContext()) {
            context.createProducer().send(context.createQueue("sendto-input"), "hello");
        }

        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            try (JMSContext context = connectionFactory.createContext()) {
                String reply = context.createConsumer(context.createQueue("sendto-output"))
                        .receiveBody(String.class, 2000);
                assertThat(reply).isEqualTo("HELLO");
            }
        });
    }

    // --- JmsResponse topic routing ---

    @Test
    void jmsResponseForTopicRoutesToTopic() throws Exception {
        try (JMSContext subCtx = connectionFactory.createContext()) {
            Topic responseTopic = subCtx.createTopic("response-topic");
            var topicConsumer = subCtx.createConsumer(responseTopic);

            try (JMSContext sendCtx = connectionFactory.createContext()) {
                sendCtx.createProducer().send(sendCtx.createQueue("topic-router-input"), "route-me");
            }

            AtomicReference<String> received = new AtomicReference<>();
            await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
                String body = topicConsumer.receiveBody(String.class, 1000);
                if (body != null) {
                    received.set(body);
                }
                assertThat(received.get()).isEqualTo("ROUTE-ME");
            });
        }

        try (JMSContext ctx = connectionFactory.createContext()) {
            String queueMsg = ctx.createConsumer(ctx.createQueue("response-topic"))
                    .receiveBody(String.class, 500);
            assertThat(queueMsg).isNull();
        }
    }

    // --- inner beans ---

    @ApplicationScoped
    public static class SendToListener {

        @JmsListener("sendto-input")
        @SendTo("sendto-output")
        public String onMessage(String message) {
            return message.toUpperCase();
        }
    }

    @ApplicationScoped
    public static class TopicRouter {

        @JmsListener("topic-router-input")
        public JmsResponse<String> onMessage(String message) {
            return JmsResponse.forTopic(message.toUpperCase(), "response-topic");
        }
    }
}
