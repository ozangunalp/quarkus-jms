package io.quarkiverse.it.jms;

import java.util.Map;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.ToxiproxyContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;

public class ArtemisTestResource implements QuarkusTestResourceLifecycleManager {

    private static Network network;
    private static GenericContainer<?> artemis;
    private static ToxiproxyContainer toxiproxy;
    private static ToxiproxyContainer.ContainerProxy proxy;

    @Override
    public Map<String, String> start() {
        network = Network.newNetwork();

        artemis = new GenericContainer<>(
                DockerImageName.parse("quay.io/artemiscloud/activemq-artemis-broker:1.0.25"))
                .withNetwork(network)
                .withNetworkAliases("artemis")
                .withExposedPorts(61616)
                .withEnv("AMQ_USER", "admin")
                .withEnv("AMQ_PASSWORD", "admin")
                .withEnv("AMQ_EXTRA_ARGS", "--no-autotune --mapped --no-fsync --relax-jolokia")
                .waitingFor(Wait.forLogMessage(".*AMQ241004.*", 1));
        artemis.start();

        toxiproxy = new ToxiproxyContainer(
                DockerImageName.parse("ghcr.io/shopify/toxiproxy:2.12.0"))
                .withNetwork(network);
        toxiproxy.start();

        proxy = toxiproxy.getProxy("artemis", 61616);

        String url = "tcp://" + proxy.getContainerIpAddress() + ":" + proxy.getProxyPort()
                + "?connectionTTL=5000&clientFailureCheckPeriod=1000";
        return Map.of(
                "quarkus.artemis.url", url,
                "quarkus.artemis.devservices.enabled", "false");
    }

    @Override
    public void stop() {
        if (toxiproxy != null) {
            toxiproxy.stop();
        }
        if (artemis != null) {
            artemis.stop();
        }
        if (network != null) {
            network.close();
        }
    }

    public static void cutConnection() {
        proxy.setConnectionCut(true);
    }

    public static void restoreConnection() {
        proxy.setConnectionCut(false);
    }
}
