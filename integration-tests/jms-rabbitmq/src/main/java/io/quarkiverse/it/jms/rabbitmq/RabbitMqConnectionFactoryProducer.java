package io.quarkiverse.it.jms.rabbitmq;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Singleton;
import jakarta.jms.ConnectionFactory;

import org.eclipse.microprofile.config.inject.ConfigProperty;

import com.rabbitmq.jms.admin.RMQConnectionFactory;

@ApplicationScoped
public class RabbitMqConnectionFactoryProducer {

    @Produces
    @Singleton
    ConnectionFactory connectionFactory(
            @ConfigProperty(name = "rabbitmq.host") String host,
            @ConfigProperty(name = "rabbitmq.port") int port,
            @ConfigProperty(name = "rabbitmq.username", defaultValue = "guest") String username,
            @ConfigProperty(name = "rabbitmq.password", defaultValue = "guest") String password) {
        RMQConnectionFactory cf = new RMQConnectionFactory();
        cf.setHost(host);
        cf.setPort(port);
        cf.setUsername(username);
        cf.setPassword(password);
        return cf;
    }
}
