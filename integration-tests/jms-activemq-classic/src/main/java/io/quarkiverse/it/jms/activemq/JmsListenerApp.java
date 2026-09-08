package io.quarkiverse.it.jms.activemq;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import javax.sql.DataSource;

import jakarta.inject.Inject;

import io.quarkiverse.jms.JmsListener;

public class JmsListenerApp {

    @Inject
    DataSource dataSource;

    private final List<String> received = new CopyOnWriteArrayList<>();
    private final List<String> xaReceived = new CopyOnWriteArrayList<>();

    @JmsListener("listener-queue")
    void onMessage(String message) {
        received.add(message);
    }

    @JmsListener(value = "xa-listener-queue", acknowledge = JmsListener.AcknowledgeMode.XA)
    void onXaMessage(String message) {
        try (var conn = dataSource.getConnection();
                var stmt = conn.prepareStatement("INSERT INTO xa_messages (payload) VALUES (?)")) {
            stmt.setString(1, "listener-xa:" + message);
            stmt.executeUpdate();
        } catch (Exception e) {
            throw new RuntimeException("DB insert failed", e);
        }
        xaReceived.add(message);
    }

    public List<String> getReceived() {
        return received;
    }

    public List<String> getXaReceived() {
        return xaReceived;
    }
}
