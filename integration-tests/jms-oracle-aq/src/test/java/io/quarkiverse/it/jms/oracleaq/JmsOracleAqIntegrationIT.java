package io.quarkiverse.it.jms.oracleaq;

import org.junit.jupiter.api.Disabled;

import io.quarkus.test.junit.QuarkusIntegrationTest;

@QuarkusIntegrationTest
@Disabled("Oracle AQ requires native image configuration")
public class JmsOracleAqIntegrationIT extends JmsOracleAqIntegrationTest {
}
