package io.quarkiverse.it.jms.activemq;

import org.junit.jupiter.api.Disabled;

import io.quarkus.test.junit.QuarkusIntegrationTest;

@QuarkusIntegrationTest
@Disabled("ActiveMQ Classic client requires native-image configuration not provided by this extension")
public class JmsActiveMqIntegrationIT extends JmsActiveMqIntegrationTest {
}
