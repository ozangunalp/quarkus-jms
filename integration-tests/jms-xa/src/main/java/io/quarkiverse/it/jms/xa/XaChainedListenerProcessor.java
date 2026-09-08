package io.quarkiverse.it.jms.xa;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import javax.sql.DataSource;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.jboss.logging.Logger;

import io.quarkiverse.jms.JmsClient;
import io.quarkiverse.jms.JmsListener;

@ApplicationScoped
public class XaChainedListenerProcessor {

    private static final Logger LOG = Logger.getLogger(XaChainedListenerProcessor.class);

    @Inject
    DataSource dataSource;

    @Inject
    JmsClient jmsClient;

    private final List<String> stageAProcessed = new CopyOnWriteArrayList<>();
    private final List<String> stageBProcessed = new CopyOnWriteArrayList<>();

    @JmsListener(value = "chain-stage-a", acknowledge = JmsListener.AcknowledgeMode.XA)
    public void stageA(String payload) {
        LOG.infof("Chain stage A: %s", payload);
        try (var conn = dataSource.getConnection();
                var stmt = conn.prepareStatement("INSERT INTO xa_messages (payload) VALUES (?)")) {
            stmt.setString(1, "chain-a:" + payload);
            stmt.executeUpdate();
        } catch (Exception e) {
            throw new RuntimeException("DB insert failed in stage A", e);
        }
        jmsClient.queue("chain-stage-b").send("from-a:" + payload);
        stageAProcessed.add(payload);
    }

    @JmsListener(value = "chain-stage-b", acknowledge = JmsListener.AcknowledgeMode.XA)
    public void stageB(String payload) {
        LOG.infof("Chain stage B: %s", payload);
        try (var conn = dataSource.getConnection();
                var stmt = conn.prepareStatement("INSERT INTO xa_messages (payload) VALUES (?)")) {
            stmt.setString(1, "chain-b:" + payload);
            stmt.executeUpdate();
        } catch (Exception e) {
            throw new RuntimeException("DB insert failed in stage B", e);
        }
        stageBProcessed.add(payload);
    }

    @JmsListener(value = "chain-stage-a-fail", acknowledge = JmsListener.AcknowledgeMode.XA)
    public void stageAFail(String payload) {
        LOG.infof("Chain stage A (will fail): %s", payload);
        try (var conn = dataSource.getConnection();
                var stmt = conn.prepareStatement("INSERT INTO xa_messages (payload) VALUES (?)")) {
            stmt.setString(1, "chain-a-fail:" + payload);
            stmt.executeUpdate();
        } catch (Exception e) {
            throw new RuntimeException("DB insert failed", e);
        }
        jmsClient.queue("chain-stage-b").send("from-a-fail:" + payload);
        throw new RuntimeException("Intentional failure in stage A");
    }

    public List<String> getStageAProcessed() {
        return stageAProcessed;
    }

    public List<String> getStageBProcessed() {
        return stageBProcessed;
    }
}
