package io.quarkiverse.jms.deployment.test;

import static org.awaitility.Awaitility.await;

import java.util.Set;

import org.apache.activemq.artemis.core.security.CheckType;
import org.apache.activemq.artemis.core.security.Role;
import org.apache.activemq.artemis.core.server.embedded.EmbeddedActiveMQ;
import org.apache.activemq.artemis.spi.core.security.ActiveMQSecurityManager;

public class EmbeddedJmsBroker {

    private static EmbeddedActiveMQ server;

    private EmbeddedJmsBroker() {
    }

    public static void start() {
        try {
            server = new EmbeddedActiveMQ();
            server.setSecurityManager(new ActiveMQSecurityManager() {
                @Override
                public boolean validateUser(String user, String password) {
                    return true;
                }

                @Override
                public boolean validateUserAndRole(String user, String password,
                        Set<Role> roles, CheckType checkType) {
                    return true;
                }
            });
            server.start();
            await().until(() -> server.getActiveMQServer().isStarted());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static void stop() {
        try {
            if (server != null) {
                server.stop();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
