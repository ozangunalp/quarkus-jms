package io.quarkiverse.it.jms.rabbitmq;

import java.util.Map;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;

public class RabbitMqTestResource implements QuarkusTestResourceLifecycleManager {

    private GenericContainer<?> rabbitmq;

    @Override
    public Map<String, String> start() {
        rabbitmq = new GenericContainer<>(DockerImageName.parse("rabbitmq:4.1"))
                .withExposedPorts(5672)
                .waitingFor(Wait.forLogMessage(".*Server startup complete.*", 1));
        rabbitmq.start();

        return Map.of(
                "rabbitmq.host", rabbitmq.getHost(),
                "rabbitmq.port", String.valueOf(rabbitmq.getMappedPort(5672)));
    }

    @Override
    public void stop() {
        if (rabbitmq != null) {
            rabbitmq.stop();
        }
    }
}
