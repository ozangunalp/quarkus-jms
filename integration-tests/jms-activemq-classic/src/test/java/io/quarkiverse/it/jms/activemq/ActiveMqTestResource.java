package io.quarkiverse.it.jms.activemq;

import java.util.Map;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;

public class ActiveMqTestResource implements QuarkusTestResourceLifecycleManager {

    private GenericContainer<?> activemq;

    @Override
    public Map<String, String> start() {
        activemq = new GenericContainer<>(
                DockerImageName.parse("apache/activemq-classic:6.1.0"))
                .withExposedPorts(61616)
                .waitingFor(Wait.forLogMessage(".*Connector openwire started.*", 1));
        activemq.start();

        return Map.of(
                "activemq.url", "tcp://" + activemq.getHost() + ":" + activemq.getMappedPort(61616));
    }

    @Override
    public void stop() {
        if (activemq != null) {
            activemq.stop();
        }
    }
}
