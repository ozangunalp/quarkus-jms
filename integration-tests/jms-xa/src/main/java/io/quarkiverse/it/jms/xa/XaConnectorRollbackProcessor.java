package io.quarkiverse.it.jms.xa;

import javax.sql.DataSource;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.jboss.logging.Logger;

@ApplicationScoped
public class XaConnectorRollbackProcessor {

    private static final Logger LOG = Logger.getLogger(XaConnectorRollbackProcessor.class);

    @Inject
    DataSource dataSource;

    @Incoming("xa-rollback-input")
    public void process(String payload) {
        LOG.infof("Processing XA connector rollback message: %s", payload);
        try (var connection = dataSource.getConnection();
                var stmt = connection.prepareStatement("INSERT INTO xa_messages (payload) VALUES (?)")) {
            stmt.setString(1, "should-not-persist-connector:" + payload);
            stmt.executeUpdate();
        } catch (Exception e) {
            throw new RuntimeException("DB insert failed", e);
        }
        throw new RuntimeException("Intentional failure to trigger XA rollback");
    }
}
