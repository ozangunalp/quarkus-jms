package io.quarkiverse.jms.deployment.test;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import jakarta.inject.Inject;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.JMSContext;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.quarkiverse.jms.JmsClient;
import io.quarkiverse.jms.JmsQueue;
import io.quarkus.test.QuarkusExtensionTest;

public class JmsReceiverTest {

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
    void multiReceiveWithSingleConsumer() {
        JmsQueue<String> queue = jmsClient.queue("receiver-test", String.class);
        for (int i = 0; i < 5; i++) {
            queue.send("msg-" + i);
        }

        List<String> received = new ArrayList<>();
        queue.receiveWith(receiver -> {
            for (int i = 0; i < 5; i++) {
                receiver.receive().ifPresent(received::add);
            }
        });

        assertThat(received).containsExactly("msg-0", "msg-1", "msg-2", "msg-3", "msg-4");
    }

    @Test
    void receiveWithReturnValue() {
        JmsQueue<String> queue = jmsClient.queue("receiver-return-test", String.class);
        queue.send("collect-me");
        queue.send("collect-me-too");

        List<String> collected = queue.receiveWith(receiver -> {
            List<String> results = new ArrayList<>();
            receiver.receive().ifPresent(results::add);
            receiver.receive().ifPresent(results::add);
            return results;
        });

        assertThat(collected).containsExactly("collect-me", "collect-me-too");
    }

    @Test
    void receiveWithSelector() {
        try (JMSContext ctx = connectionFactory.createContext()) {
            var dest = ctx.createQueue("receiver-selector-test");
            ctx.createProducer().setProperty("level", "HIGH").send(dest, "important");
            ctx.createProducer().setProperty("level", "LOW").send(dest, "trivial");
            ctx.createProducer().setProperty("level", "HIGH").send(dest, "urgent");
        }

        List<String> received = jmsClient.queue("receiver-selector-test", String.class)
                .select("level = 'HIGH'")
                .receiveWith(receiver -> {
                    List<String> results = new ArrayList<>();
                    receiver.receive().ifPresent(results::add);
                    receiver.receive().ifPresent(results::add);
                    return results;
                });

        assertThat(received).containsExactly("important", "urgent");
    }

    @Test
    void receiveWithTimeout() {
        JmsQueue<String> queue = jmsClient.queue("receiver-timeout-test", String.class);

        queue.receiveTimeout(Duration.ofMillis(200)).receiveWith(receiver -> {
            Optional<String> result = receiver.receive();
            assertThat(result).isEmpty();
        });
    }

    @Test
    void receiveWithMessage() {
        JmsQueue<String> queue = jmsClient.queue("receiver-msg-test", String.class);
        queue.send("with-headers");

        queue.receiveWith(receiver -> {
            receiver.receiveMessage().ifPresent(msg -> {
                assertThat(msg.payload()).isEqualTo("with-headers");
                assertThat(msg.message()).isNotNull();
            });
        });
    }

    @Test
    void sessionReturnedToPoolAfterCallback() {
        JmsQueue<String> queue = jmsClient.queue("receiver-pool-test", String.class);
        queue.send("before");

        queue.receiveWith(receiver -> {
            receiver.receive();
        });

        queue.send("after");
        Optional<String> result = queue.receive();
        assertThat(result).contains("after");
    }
}
