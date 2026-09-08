package io.quarkiverse.it.jms.ibmmq;

import org.junit.jupiter.api.Disabled;

import io.quarkus.test.junit.QuarkusIntegrationTest;

@QuarkusIntegrationTest
@Disabled("IBM MQ client requires native image configuration not yet available")
public class JmsIbmMqIntegrationIT extends JmsIbmMqIntegrationTest {
}
