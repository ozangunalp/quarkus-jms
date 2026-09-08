package io.quarkiverse.it.jms.xa;

import javax.sql.DataSource;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.jboss.logging.Logger;

import io.quarkiverse.jms.JmsListener;

@ApplicationScoped
public class XaListenerProcessor {

    private static final Logger LOG = Logger.getLogger(XaListenerProcessor.class);

    @Inject
    DataSource dataSource;

    @JmsListener(value = "xa-listener-queue", acknowledge = JmsListener.AcknowledgeMode.XA)
    public void process(String payload) {
        LOG.infof("Processing XA listener message: %s", payload);
        try (var connection = dataSource.getConnection();
                var stmt = connection.prepareStatement("INSERT INTO xa_messages (payload) VALUES (?)")) {
            stmt.setString(1, "listener:" + payload);
            stmt.executeUpdate();
        } catch (Exception e) {
            throw new RuntimeException("Failed to insert into database", e);
        }
    }

    @JmsListener(value = "xa-listener-rollback-queue", acknowledge = JmsListener.AcknowledgeMode.XA)
    public void processAndFail(String payload) {
        LOG.infof("Processing XA listener message (will fail): %s", payload);
        try (var connection = dataSource.getConnection();
                var stmt = connection.prepareStatement("INSERT INTO xa_messages (payload) VALUES (?)")) {
            stmt.setString(1, "should-not-persist-listener:" + payload);
            stmt.executeUpdate();
        } catch (Exception e) {
            throw new RuntimeException("Failed to insert into database", e);
        }
        throw new RuntimeException("Intentional failure to trigger XA rollback");
    }
}
