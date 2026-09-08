package io.quarkiverse.jms.runtime;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.eclipse.microprofile.health.HealthCheck;
import org.eclipse.microprofile.health.HealthCheckResponse;
import org.eclipse.microprofile.health.Readiness;

@Readiness
@ApplicationScoped
public class JmsHealthCheck implements HealthCheck {

    @Inject
    JmsListenerRegistryImpl listenerContainer;

    @Override
    public HealthCheckResponse call() {
        return HealthCheckResponse.named("JMS listeners")
                .status(listenerContainer.isHealthy())
                .build();
    }
}
