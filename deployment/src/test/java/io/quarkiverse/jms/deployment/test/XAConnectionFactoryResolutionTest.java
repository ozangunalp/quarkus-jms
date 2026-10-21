package io.quarkiverse.jms.deployment.test;

import static org.assertj.core.api.Assertions.assertThat;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.XAConnectionFactory;

import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.quarkiverse.jms.runtime.JmsConnectionFactoryResolver;
import io.quarkus.arc.Unremovable;
import io.quarkus.test.QuarkusExtensionTest;
import io.smallrye.common.annotation.Identifier;

public class XAConnectionFactoryResolutionTest {

    @RegisterExtension
    static final QuarkusExtensionTest config = new QuarkusExtensionTest()
            .overrideConfigKey("quarkus.http.test-port", "0")
            .withApplicationRoot(jar -> jar
                    .addClasses(CfProducer.class, EmbeddedJmsBroker.class)
                    .addAsResource("broker.xml"))
            .setBeforeAllCustomizer(EmbeddedJmsBroker::start)
            .setAfterAllCustomizer(EmbeddedJmsBroker::stop);

    @Inject
    JmsConnectionFactoryResolver resolver;

    @Test
    void defaultConnectionFactoryResolvesAsXA() {
        XAConnectionFactory xa = resolver.resolveXA(null);
        assertThat(xa).isNotNull();
        assertThat(xa).isInstanceOf(ActiveMQConnectionFactory.class);
    }

    @Test
    void namedConnectionFactoryResolvesAsXA() {
        XAConnectionFactory xa = resolver.resolveXA("broker-b");
        assertThat(xa).isNotNull();
        assertThat(xa).isInstanceOf(ActiveMQConnectionFactory.class);
    }

    @ApplicationScoped
    public static class CfProducer {

        @Produces
        @ApplicationScoped
        ConnectionFactory defaultCf() {
            return new ActiveMQConnectionFactory("tcp://localhost:61616");
        }

        @Produces
        @ApplicationScoped
        @Identifier("broker-b")
        @Unremovable
        ConnectionFactory brokerB() {
            return new ActiveMQConnectionFactory("tcp://localhost:61616");
        }
    }
}
