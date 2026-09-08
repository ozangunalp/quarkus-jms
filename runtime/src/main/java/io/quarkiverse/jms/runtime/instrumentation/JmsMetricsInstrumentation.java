package io.quarkiverse.jms.runtime.instrumentation;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.jms.Message;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import io.quarkiverse.jms.runtime.JmsInstrumentation;
import io.quarkiverse.jms.runtime.SessionPoolListener;

@ApplicationScoped
public class JmsMetricsInstrumentation implements JmsInstrumentation, SessionPoolListener {

    @Inject
    MeterRegistry registry;

    private final ConcurrentHashMap<String, DestinationMetrics> cache = new ConcurrentHashMap<>();
    private final Map<String, PoolCounters> poolCounters = new ConcurrentHashMap<>();

    // --- JmsInstrumentation ---

    @Override
    public Scope onSend(String destination, boolean isTopic) {
        return new MetricsScope(destination, Timer.start(registry), MetricsScope.Operation.SEND);
    }

    @Override
    public Scope onReceive(String destination, boolean isTopic) {
        return new MetricsScope(destination, Timer.start(registry), MetricsScope.Operation.RECEIVE);
    }

    @Override
    public Scope onProcess(String destination, boolean isTopic) {
        return new MetricsScope(destination, Timer.start(registry), MetricsScope.Operation.PROCESS);
    }

    // --- SessionPoolListener ---

    @Override
    public void onPoolCreated(String factoryId, int maxSessions) {
        PoolCounters counters = new PoolCounters();
        poolCounters.put(factoryId, counters);
        Tags tags = Tags.of("factory", factoryId);

        Gauge.builder("jms.pool.active", counters.active, LongAdder::doubleValue)
                .tags(tags).description("Sessions currently in use").register(registry);

        Gauge.builder("jms.pool.idle", counters, c -> c.total.sum() - c.active.sum())
                .tags(tags).description("Idle sessions in pool").register(registry);

        Gauge.builder("jms.pool.borrow.total", counters.borrowed, LongAdder::doubleValue)
                .tags(tags).description("Total sessions borrowed").register(registry);

        Gauge.builder("jms.pool.timeout.total", counters.timedOut, LongAdder::doubleValue)
                .tags(tags).description("Borrow attempts that timed out").register(registry);
    }

    @Override
    public void onSessionCreated(String factoryId) {
        PoolCounters counters = poolCounters.get(factoryId);
        if (counters != null) {
            counters.total.increment();
        }
    }

    @Override
    public void onSessionClosed(String factoryId) {
        PoolCounters counters = poolCounters.get(factoryId);
        if (counters != null) {
            counters.total.decrement();
        }
    }

    @Override
    public void onSessionBorrowed(String factoryId) {
        PoolCounters counters = poolCounters.get(factoryId);
        if (counters != null) {
            counters.active.increment();
            counters.borrowed.increment();
        }
    }

    @Override
    public void onSessionReturned(String factoryId) {
        PoolCounters counters = poolCounters.get(factoryId);
        if (counters != null) {
            counters.active.decrement();
        }
    }

    @Override
    public void onBorrowTimeout(String factoryId) {
        PoolCounters counters = poolCounters.get(factoryId);
        if (counters != null) {
            counters.timedOut.increment();
        }
    }

    // --- Internal ---

    private DestinationMetrics metersFor(String destination) {
        return cache.computeIfAbsent(destination, d -> new DestinationMetrics(registry, d));
    }

    private static class PoolCounters {
        final LongAdder total = new LongAdder();
        final LongAdder active = new LongAdder();
        final LongAdder borrowed = new LongAdder();
        final LongAdder timedOut = new LongAdder();
    }

    private class MetricsScope implements Scope {

        enum Operation {
            SEND,
            RECEIVE,
            PROCESS
        }

        private final String destination;
        private final Timer.Sample sample;
        private final Operation operation;
        private boolean error;
        private boolean messageReceived;

        MetricsScope(String destination, Timer.Sample sample, Operation operation) {
            this.destination = destination;
            this.sample = sample;
            this.operation = operation;
        }

        @Override
        public void onMessage(Message message) {
            messageReceived = message != null;
        }

        @Override
        public void onError(Throwable t) {
            error = true;
        }

        @Override
        public void close() {
            DestinationMetrics meters = metersFor(destination);
            switch (operation) {
                case SEND -> sample.stop(meters.send(!error));
                case RECEIVE -> sample.stop(meters.receive(messageReceived));
                case PROCESS -> sample.stop(meters.listener(!error));
            }
        }
    }

    private static class DestinationMetrics {

        private final Timer sendSuccess;
        private final Timer sendError;
        private final Timer receiveReceived;
        private final Timer receiveEmpty;
        private final Timer listenerSuccess;
        private final Timer listenerError;

        DestinationMetrics(MeterRegistry registry, String destination) {
            Tags tags = Tags.of("destination", destination);
            this.sendSuccess = Timer.builder("jms.send").tags(tags.and("outcome", "success")).register(registry);
            this.sendError = Timer.builder("jms.send").tags(tags.and("outcome", "error")).register(registry);
            this.receiveReceived = Timer.builder("jms.receive").tags(tags.and("outcome", "received")).register(registry);
            this.receiveEmpty = Timer.builder("jms.receive").tags(tags.and("outcome", "empty")).register(registry);
            this.listenerSuccess = Timer.builder("jms.listener").tags(tags.and("outcome", "success")).register(registry);
            this.listenerError = Timer.builder("jms.listener").tags(tags.and("outcome", "error")).register(registry);
        }

        Timer send(boolean success) {
            return success ? sendSuccess : sendError;
        }

        Timer receive(boolean received) {
            return received ? receiveReceived : receiveEmpty;
        }

        Timer listener(boolean success) {
            return success ? listenerSuccess : listenerError;
        }
    }
}
