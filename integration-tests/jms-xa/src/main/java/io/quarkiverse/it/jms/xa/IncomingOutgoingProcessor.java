package io.quarkiverse.it.jms.xa;

import javax.sql.DataSource;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.jboss.logging.Logger;

/**
 * Receives from one queue, writes to DB, and forwards to another queue.
 * All three operations (JMS consume, DB write, JMS send) should be in
 * the same transaction when transaction-mode is local or xa.
 */
@ApplicationScoped
public class IncomingOutgoingProcessor {

    private static final Logger LOG = Logger.getLogger(IncomingOutgoingProcessor.class);

    @Inject
    DataSource dataSource;

    @Incoming("process-in")
    @Outgoing("process-out")
    public String process(String payload) {
        LOG.infof("Processing incoming-outgoing: %s", payload);
        try (var conn = dataSource.getConnection();
                var stmt = conn.prepareStatement("INSERT INTO xa_messages (payload) VALUES (?)")) {
            stmt.setString(1, "processed:" + payload);
            stmt.executeUpdate();
        } catch (Exception e) {
            throw new RuntimeException("DB insert failed", e);
        }
        return "forwarded:" + payload;
    }
}
