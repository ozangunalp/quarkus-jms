package io.quarkiverse.jms.deployment.test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.TimeUnit;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.JMSContext;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.quarkiverse.jms.JmsListener;
import io.quarkus.test.QuarkusExtensionTest;
import io.smallrye.common.vertx.VertxContext;
import io.vertx.core.Context;
import io.vertx.core.Vertx;

public class JmsListenerDuplicatedContextTest {

    @RegisterExtension
    static final QuarkusExtensionTest config = new QuarkusExtensionTest()
            .withApplicationRoot(jar -> jar
                    .addClasses(ContextCheckingListener.class, ConnectionFactoryProducer.class,
                            EmbeddedJmsBroker.class)
                    .addAsResource("broker.xml"))
            .setBeforeAllCustomizer(EmbeddedJmsBroker::start)
            .setAfterAllCustomizer(EmbeddedJmsBroker::stop);

    @Inject
    ConnectionFactory connectionFactory;

    @Inject
    ContextCheckingListener listener;

    @Test
    void eachMessageGetsItsOwnDuplicatedContext() throws Exception {
        Thread.sleep(500);

        try (JMSContext context = connectionFactory.createContext()) {
            for (int i = 0; i < 5; i++) {
                context.createProducer().send(context.createQueue("context-check-queue"), "msg-" + i);
            }
        }

        await().atMost(10, TimeUnit.SECONDS)
                .until(() -> listener.getMessages().size() >= 5);

        assertThat(listener.allOnDuplicatedContext())
                .as("Every message should be processed on a duplicated context")
                .isTrue();

        assertThat(listener.getContextIdentities())
                .as("Each message should get a distinct duplicated context")
                .hasSizeGreaterThanOrEqualTo(5);
    }

    @ApplicationScoped
    public static class ContextCheckingListener {

        private final CopyOnWriteArrayList<String> messages = new CopyOnWriteArrayList<>();
        private final CopyOnWriteArrayList<Boolean> duplicatedFlags = new CopyOnWriteArrayList<>();
        private final CopyOnWriteArraySet<Integer> contextIdentities = new CopyOnWriteArraySet<>();

        @JmsListener("context-check-queue")
        public void onMessage(String message) {
            messages.add(message);
            duplicatedFlags.add(VertxContext.isOnDuplicatedContext());
            Context current = Vertx.currentContext();
            if (current != null) {
                contextIdentities.add(System.identityHashCode(current));
            }
        }

        public List<String> getMessages() {
            return messages;
        }

        public boolean allOnDuplicatedContext() {
            return !duplicatedFlags.isEmpty() && duplicatedFlags.stream().allMatch(b -> b);
        }

        public Set<Integer> getContextIdentities() {
            return contextIdentities;
        }
    }
}
