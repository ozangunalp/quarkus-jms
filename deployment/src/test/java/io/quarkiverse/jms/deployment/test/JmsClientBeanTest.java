package io.quarkiverse.jms.deployment.test;

import static org.assertj.core.api.Assertions.assertThat;

import jakarta.inject.Inject;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.quarkiverse.jms.JmsClient;
import io.quarkus.test.QuarkusExtensionTest;

public class JmsClientBeanTest {

    @RegisterExtension
    static final QuarkusExtensionTest config = new QuarkusExtensionTest()
            .withApplicationRoot(jar -> jar.addClasses(ConnectionFactoryProducer.class));

    @Inject
    JmsClient jmsClient;

    @Test
    void jmsClientIsInjectable() {
        assertThat(jmsClient).isNotNull();
        assertThat(jmsClient.queue("test")).isNotNull();
        assertThat(jmsClient.topic("test")).isNotNull();
    }
}
