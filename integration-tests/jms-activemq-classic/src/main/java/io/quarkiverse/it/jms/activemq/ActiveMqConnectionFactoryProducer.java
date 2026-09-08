package io.quarkiverse.it.jms.activemq;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Singleton;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.XAConnectionFactory;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQXAConnectionFactory;
import org.eclipse.microprofile.config.inject.ConfigProperty;

@ApplicationScoped
public class ActiveMqConnectionFactoryProducer {

    @Produces
    @Singleton
    ConnectionFactory connectionFactory(
            @ConfigProperty(name = "activemq.url") String brokerUrl) {
        return new ActiveMQConnectionFactory(brokerUrl);
    }

    @Produces
    @Singleton
    XAConnectionFactory xaConnectionFactory(
            @ConfigProperty(name = "activemq.url") String brokerUrl) {
        return new ActiveMQXAConnectionFactory(brokerUrl);
    }
}
