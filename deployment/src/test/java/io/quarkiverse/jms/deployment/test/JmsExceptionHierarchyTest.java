package io.quarkiverse.jms.deployment.test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;

import jakarta.inject.Inject;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.quarkiverse.jms.JmsClient;
import io.quarkiverse.jms.JmsConversionException;
import io.quarkiverse.jms.JmsException;
import io.quarkus.test.QuarkusExtensionTest;

public class JmsExceptionHierarchyTest {

    @RegisterExtension
    static final QuarkusExtensionTest config = new QuarkusExtensionTest()
            .withApplicationRoot(jar -> jar
                    .addClasses(ConnectionFactoryProducer.class, EmbeddedJmsBroker.class)
                    .addAsResource("broker.xml"))
            .setBeforeAllCustomizer(EmbeddedJmsBroker::start)
            .setAfterAllCustomizer(EmbeddedJmsBroker::stop);

    @Inject
    JmsClient jmsClient;

    @Test
    void jmsExceptionHierarchyExists() {
        assertThat(JmsException.class).isAssignableFrom(io.quarkiverse.jms.JmsSendException.class);
        assertThat(JmsException.class).isAssignableFrom(io.quarkiverse.jms.JmsReceiveException.class);
        assertThat(JmsException.class).isAssignableFrom(JmsConversionException.class);
        assertThat(JmsException.class).isAssignableFrom(io.quarkiverse.jms.JmsListenerException.class);
        assertThat(RuntimeException.class).isAssignableFrom(JmsException.class);
    }

    @Test
    void sendAndReceiveTimeoutThrowsJmsReceiveException() {
        assertThatThrownBy(() -> jmsClient.queue("empty-rr-queue")
                .message("request")
                .receiveTimeout(Duration.ofMillis(500))
                .sendAndReceive(String.class))
                .isInstanceOf(io.quarkiverse.jms.JmsReceiveException.class)
                .hasMessageContaining("Timed out");
    }
}
