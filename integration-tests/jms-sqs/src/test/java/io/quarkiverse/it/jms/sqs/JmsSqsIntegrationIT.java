package io.quarkiverse.it.jms.sqs;

import org.junit.jupiter.api.Disabled;

import io.quarkus.test.junit.QuarkusIntegrationTest;

@QuarkusIntegrationTest
@Disabled("SQS JMS client requires native image configuration")
public class JmsSqsIntegrationIT extends JmsSqsIntegrationTest {
}
