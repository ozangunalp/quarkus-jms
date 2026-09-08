package io.quarkiverse.spring.jms.deployment;

import java.util.Map;
import java.util.Optional;

import io.quarkus.runtime.annotations.ConfigPhase;
import io.quarkus.runtime.annotations.ConfigRoot;
import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;

@ConfigMapping(prefix = "quarkus.spring.jms")
@ConfigRoot(phase = ConfigPhase.BUILD_TIME)
public interface SpringJmsBuildTimeConfig {

    /**
     * Whether destinations are topics (pub/sub) by default.
     * When {@code true}, all Spring {@code @JmsListener} destinations and
     * {@code JmsClient.destination()} calls use topics instead of queues.
     */
    @WithDefault("false")
    boolean pubSubDomain();

    /**
     * Named container factory configurations. Maps Spring's
     * {@code @JmsListener(containerFactory = "name")} to Quarkus config.
     *
     * Example:
     *
     * <pre>
     * quarkus.spring.jms.container-factories.topicFactory.pub-sub-domain=true
     * quarkus.spring.jms.container-factories.topicFactory.concurrency=3-10
     * </pre>
     */
    Map<String, ContainerFactoryConfig> containerFactories();

    interface ContainerFactoryConfig {

        /**
         * Whether this factory uses topics (pub/sub) instead of queues.
         */
        @WithDefault("false")
        boolean pubSubDomain();

        /**
         * Concurrency for listeners using this factory.
         * Supports Spring's range format (e.g., "3-10").
         */
        Optional<String> concurrency();

        /**
         * Acknowledge mode for listeners using this factory.
         */
        Optional<String> acknowledgeMode();
    }
}
