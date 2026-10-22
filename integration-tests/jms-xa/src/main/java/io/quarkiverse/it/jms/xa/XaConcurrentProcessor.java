package io.quarkiverse.it.jms.xa;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import javax.sql.DataSource;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.jboss.logging.Logger;

import io.smallrye.common.annotation.RunOnVirtualThread;

@ApplicationScoped
public class XaConcurrentProcessor {

    private static final Logger LOG = Logger.getLogger(XaConcurrentProcessor.class);

    @Inject
    DataSource dataSource;

    private final AtomicInteger inflight = new AtomicInteger();
    private final AtomicInteger maxInflight = new AtomicInteger();
    private final CopyOnWriteArrayList<String> processed = new CopyOnWriteArrayList<>();

    @Incoming("xa-concurrent")
    @RunOnVirtualThread
    public void process(String payload) {
        int current = inflight.incrementAndGet();
        maxInflight.updateAndGet(max -> Math.max(max, current));
        LOG.infof("XA concurrent processing: %s (inflight=%d, thread=%s)",
                payload, current, Thread.currentThread().getName());
        try {
            try (var connection = dataSource.getConnection();
                    var stmt = connection.prepareStatement("INSERT INTO xa_messages (payload) VALUES (?)")) {
                stmt.setString(1, "concurrent:" + payload);
                stmt.executeUpdate();
            }
            Thread.sleep(500);
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            inflight.decrementAndGet();
            processed.add(payload);
        }
    }

    public int getMaxInflight() {
        return maxInflight.get();
    }

    public List<String> getProcessed() {
        return processed;
    }
}
