package io.quarkiverse.it.jms.ibmmq;

import java.util.Map;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;

public class IbmMqTestResource implements QuarkusTestResourceLifecycleManager {

    private GenericContainer<?> mq;

    @Override
    public Map<String, String> start() {
        mq = new GenericContainer<>(
                DockerImageName.parse("icr.io/ibm-messaging/mq:9.4.0.25-r3"))
                .withCreateContainerCmdModifier(cmd -> cmd.withPlatform("linux/amd64"))
                .withExposedPorts(1414)
                .withEnv("LICENSE", "accept")
                .withEnv("MQ_QMGR_NAME", "QM1")
                .withEnv("MQ_APP_PASSWORD", "passw0rd")
                .waitingFor(Wait.forLogMessage(".*Started web server.*", 1));
        mq.start();

        return Map.of(
                "ibmmq.host", mq.getHost(),
                "ibmmq.port", String.valueOf(mq.getMappedPort(1414)));
    }

    @Override
    public void stop() {
        if (mq != null) {
            mq.stop();
        }
    }
}
