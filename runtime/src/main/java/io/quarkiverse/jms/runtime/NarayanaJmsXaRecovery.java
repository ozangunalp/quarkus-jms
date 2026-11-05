package io.quarkiverse.jms.runtime;

import java.util.Optional;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.jms.XAConnectionFactory;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.narayana.jta.jms.JmsXAResourceRecoveryHelper;
import org.jboss.tm.XAResourceRecoveryRegistry;

import io.quarkiverse.jms.spi.JmsXaRecovery;

@ApplicationScoped
public class NarayanaJmsXaRecovery implements JmsXaRecovery {

    @Inject
    XAResourceRecoveryRegistry registry;

    @Inject
    @ConfigProperty(name = "quarkus.transaction-manager.enable-recovery")
    Optional<Boolean> recoveryEnabled;

    @Override
    public void register(XAConnectionFactory xaFactory) {
        if (!recoveryEnabled.orElse(false)) {
            return;
        }
        JmsXAResourceRecoveryHelper helper = new JmsXAResourceRecoveryHelper(xaFactory);
        registry.addXAResourceRecovery(helper::getXAResources);
    }
}
