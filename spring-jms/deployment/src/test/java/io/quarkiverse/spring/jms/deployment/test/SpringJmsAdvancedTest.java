package io.quarkiverse.spring.jms.deployment.test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.JMSContext;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.springframework.jms.annotation.JmsListener;
import org.springframework.jms.listener.adapter.JmsResponse;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;

import io.quarkus.test.QuarkusExtensionTest;

public class SpringJmsAdvancedTest {

    @RegisterExtension
    static final QuarkusExtensionTest config = new QuarkusExtensionTest()
            .withApplicationRoot(jar -> jar
                    .addClasses(AdvancedListener.class, ConnectionFactoryProducer.class,
                            EmbeddedJmsBroker.class)
                    .addAsResource("broker.xml"))
            .setBeforeAllCustomizer(EmbeddedJmsBroker::start)
            .setAfterAllCustomizer(EmbeddedJmsBroker::stop);

    @Inject
    ConnectionFactory connectionFactory;

    @Inject
    AdvancedListener listener;

    @Test
    void payloadAnnotationSelectsPayloadParameter() {
        try (JMSContext ctx = connectionFactory.createContext()) {
            var msg = ctx.createTextMessage("payload-test");
            msg.setStringProperty("tag", "important");
            ctx.createProducer().send(ctx.createQueue("payload-queue"), msg);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        await().atMost(10, TimeUnit.SECONDS)
                .until(() -> !listener.getPayloadResults().isEmpty());
        assertThat(listener.getPayloadResults()).contains("payload-test:tag=important");
    }

    @Test
    void springJmsResponseDynamicReply() {
        try (JMSContext ctx = connectionFactory.createContext()) {
            ctx.createProducer().send(ctx.createQueue("spring-dynamic-queue"), "vip:order1");
        }

        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            try (JMSContext ctx = connectionFactory.createContext()) {
                String reply = ctx.createConsumer(ctx.createQueue("spring-vip-output"))
                        .receiveBody(String.class, 5000);
                assertThat(reply).isEqualTo("processed:vip:order1");
            }
        });
    }

    @ApplicationScoped
    public static class AdvancedListener {

        private final CopyOnWriteArrayList<String> payloadResults = new CopyOnWriteArrayList<>();

        @JmsListener(destination = "payload-queue")
        public void onPayload(@Header("tag") String tag, @Payload String body) {
            payloadResults.add(body + ":tag=" + tag);
        }

        @JmsListener(destination = "spring-dynamic-queue")
        public JmsResponse<String> onDynamicReply(String body) {
            String target = body.startsWith("vip:") ? "spring-vip-output" : "spring-regular-output";
            return JmsResponse.forQueue("processed:" + body, target);
        }

        public List<String> getPayloadResults() {
            return payloadResults;
        }
    }
}
