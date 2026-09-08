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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.quarkiverse.jms.JmsClient;
import io.quarkiverse.jms.JmsListener;
import io.quarkus.test.QuarkusExtensionTest;

public class JmsBrokerFailoverTest {

    @RegisterExtension
    static final QuarkusExtensionTest config = new QuarkusExtensionTest()
            .withApplicationRoot(jar -> jar
                    .addClasses(FailoverListener.class, ConnectionFactoryProducer.class,
                            EmbeddedJmsBroker.class)
                    .addAsResource("broker.xml"))
            .setBeforeAllCustomizer(EmbeddedJmsBroker::start)
            .setAfterAllCustomizer(EmbeddedJmsBroker::stop);

    @Inject
    JmsClient jms;

    @Inject
    ConnectionFactory connectionFactory;

    @Inject
    FailoverListener listener;

    @Test
    void listenerRecoversAfterBrokerRestart() throws Exception {
        Thread.sleep(500);

        try (JMSContext ctx = connectionFactory.createContext()) {
            ctx.createProducer().send(ctx.createQueue("failover-queue"), "before-restart");
        }

        await().atMost(10, TimeUnit.SECONDS)
                .until(() -> listener.getMessages().contains("before-restart"));

        EmbeddedJmsBroker.stop();
        Thread.sleep(2000);
        EmbeddedJmsBroker.start();

        await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> {
            try (JMSContext ctx = connectionFactory.createContext()) {
                ctx.createProducer().send(ctx.createQueue("failover-queue"), "after-restart");
            }
            assertThat(listener.getMessages()).contains("after-restart");
        });
    }

    @Test
    void jmsClientRecoversAfterBrokerRestart() throws Exception {
        jms.queue("client-failover-queue").send("before");

        String received = jms.queue("client-failover-queue", String.class).receive().orElse(null);
        assertThat(received).isEqualTo("before");

        EmbeddedJmsBroker.stop();
        Thread.sleep(2000);
        EmbeddedJmsBroker.start();

        await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> {
            jms.queue("client-failover-queue").send("after");
            String msg = jms.queue("client-failover-queue", String.class).receive().orElse(null);
            assertThat(msg).isEqualTo("after");
        });
    }

    @ApplicationScoped
    public static class FailoverListener {

        private final List<String> messages = new CopyOnWriteArrayList<>();

        @JmsListener("failover-queue")
        public void onMessage(String message) {
            messages.add(message);
        }

        public List<String> getMessages() {
            return messages;
        }
    }
}
