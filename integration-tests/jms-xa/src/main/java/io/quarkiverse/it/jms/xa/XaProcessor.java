package io.quarkiverse.it.jms.xa;

import java.util.List;

import javax.sql.DataSource;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.jboss.logging.Logger;

/**
 * Receives JMS messages via the smallrye-jms connector with XA transaction mode.
 * Each message triggers an INSERT into the database. Both the JMS consume and
 * the DB write participate in the same XA transaction.
 */
@ApplicationScoped
public class XaProcessor {

    private static final Logger LOG = Logger.getLogger(XaProcessor.class);

    @Inject
    DataSource dataSource;

    @Incoming("xa-input")
    public void process(String payload) {
        LOG.infof("Processing XA message: %s", payload);
        try (var connection = dataSource.getConnection();
                var stmt = connection.prepareStatement("INSERT INTO xa_messages (payload) VALUES (?)")) {
            stmt.setString(1, payload);
            stmt.executeUpdate();
        } catch (Exception e) {
            throw new RuntimeException("Failed to insert into database", e);
        }
    }

    public List<String> getMessages() {
        try (var connection = dataSource.getConnection();
                var stmt = connection.createStatement();
                var rs = stmt.executeQuery("SELECT payload FROM xa_messages ORDER BY id")) {
            List<String> messages = new java.util.ArrayList<>();
            while (rs.next()) {
                messages.add(rs.getString("payload"));
            }
            return messages;
        } catch (Exception e) {
            throw new RuntimeException("Failed to query database", e);
        }
    }
}
