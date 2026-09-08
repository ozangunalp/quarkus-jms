package io.quarkiverse.jms.deployment;

import io.quarkus.runtime.annotations.ConfigPhase;
import io.quarkus.runtime.annotations.ConfigRoot;
import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;

@ConfigMapping(prefix = "quarkus.jms")
@ConfigRoot(phase = ConfigPhase.BUILD_TIME)
public interface JmsBuildTimeConfig {

    /**
     * Whether the JMS health check is enabled.
     */
    @WithDefault("true")
    boolean healthEnabled();
}
