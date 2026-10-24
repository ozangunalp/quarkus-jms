package io.quarkiverse.it.jms.qpid;

import java.util.Map;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;

public class QpidJmsTestResource implements QuarkusTestResourceLifecycleManager {

    private GenericContainer<?> artemis;

    @Override
    public Map<String, String> start() {
        artemis = new GenericContainer<>(
                DockerImageName.parse("quay.io/arkmq-org/arkmq-org-broker:artemis.2.55.0"))
                .withExposedPorts(5672)
                .withEnv("AMQ_USER", "quarkus")
                .withEnv("AMQ_PASSWORD", "quarkus")
                .waitingFor(Wait.forLogMessage(".*AMQ241004.*", 1));
        artemis.start();

        String url = "amqp://" + artemis.getHost() + ":" + artemis.getMappedPort(5672);
        return Map.of(
                "quarkus.qpid-jms.url", url,
                "quarkus.qpid-jms.username", "quarkus",
                "quarkus.qpid-jms.password", "quarkus");
    }

    @Override
    public void stop() {
        if (artemis != null) {
            artemis.stop();
        }
    }
}
