package io.quarkiverse.it.jms.oracleaq;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Singleton;
import jakarta.jms.ConnectionFactory;

import org.eclipse.microprofile.config.inject.ConfigProperty;

import oracle.jakarta.jms.AQjmsFactory;
import oracle.jdbc.pool.OracleDataSource;

@ApplicationScoped
public class OracleAqConnectionFactoryProducer {

    @Produces
    @Singleton
    ConnectionFactory connectionFactory(
            @ConfigProperty(name = "oracle.jdbc.url") String jdbcUrl,
            @ConfigProperty(name = "oracle.jdbc.user") String user,
            @ConfigProperty(name = "oracle.jdbc.password") String password) throws Exception {
        OracleDataSource dataSource = new OracleDataSource();
        dataSource.setURL(jdbcUrl);
        dataSource.setUser(user);
        dataSource.setPassword(password);
        return AQjmsFactory.getConnectionFactory(dataSource);
    }
}
