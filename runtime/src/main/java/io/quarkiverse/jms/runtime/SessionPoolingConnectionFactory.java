package io.quarkiverse.jms.runtime;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import jakarta.jms.Connection;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.ExceptionListener;
import jakarta.jms.JMSContext;
import jakarta.jms.JMSException;
import jakarta.jms.JMSRuntimeException;
import jakarta.jms.Session;

import org.jboss.logging.Logger;

/**
 * A pooling wrapper around a {@link ConnectionFactory} that shares a single
 * underlying connection and maintains a bounded pool of JMS sessions per
 * acknowledge mode.
 * <p>
 * Sessions are borrowed with backpressure (blocking when the pool is exhausted)
 * and returned automatically via {@link PooledJMSContext#close()}.
 */
public class SessionPoolingConnectionFactory implements ConnectionFactory, ExceptionListener {

    private static final Logger LOG = Logger.getLogger(SessionPoolingConnectionFactory.class);

    private final ConnectionFactory delegate;
    private final int maxSessions;
    private final int maxIdleSessions;
    private final long borrowTimeoutNanos; // -1 = infinite
    private final long idleTimeoutNanos;
    private final boolean cacheConsumers;
    private final boolean reconnectOnException;
    private final boolean wrapJmsContext;
    private final String factoryId;
    private final SessionPoolListener poolListener;

    private volatile JMSContext sharedContext;
    private final Object connectionLock = new Object();
    private final AtomicBoolean connectionFailed = new AtomicBoolean(false);
    private final Map<Integer, SessionModePool> pools = new ConcurrentHashMap<>();

    public SessionPoolingConnectionFactory(ConnectionFactory delegate, int maxSessions,
            int maxIdleSessions, Duration borrowTimeout, Duration idleTimeout,
            boolean cacheConsumers, boolean reconnectOnException, boolean wrapJmsContext,
            String factoryId, SessionPoolListener poolListener) {
        this.delegate = delegate;
        this.maxSessions = maxSessions;
        this.maxIdleSessions = maxIdleSessions;
        this.borrowTimeoutNanos = borrowTimeout != null ? borrowTimeout.toNanos() : -1;
        this.idleTimeoutNanos = idleTimeout.toNanos();
        this.cacheConsumers = cacheConsumers;
        this.reconnectOnException = reconnectOnException;
        this.wrapJmsContext = wrapJmsContext;
        this.factoryId = factoryId;
        this.poolListener = poolListener;
        poolListener.onPoolCreated(factoryId, maxSessions);
    }

    @Override
    public Connection createConnection() throws JMSException {
        return delegate.createConnection();
    }

    @Override
    public Connection createConnection(String userName, String password) throws JMSException {
        return delegate.createConnection(userName, password);
    }

    @Override
    public JMSContext createContext() {
        return createContext(Session.AUTO_ACKNOWLEDGE);
    }

    @Override
    public JMSContext createContext(int sessionMode) {
        SessionModePool pool = pools.computeIfAbsent(sessionMode,
                mode -> new SessionModePool(mode, maxSessions));
        return pool.borrow();
    }

    @Override
    public JMSContext createContext(String userName, String password) {
        return createContext(userName, password, Session.AUTO_ACKNOWLEDGE);
    }

    @Override
    public JMSContext createContext(String userName, String password, int sessionMode) {
        return delegate.createContext(userName, password, sessionMode);
    }

    private JMSContext getSharedContext() {
        JMSContext ctx = sharedContext;
        if (ctx != null && !connectionFailed.get()) {
            return ctx;
        }
        synchronized (connectionLock) {
            if (sharedContext == null || connectionFailed.get()) {
                if (sharedContext != null) {
                    drainAllPools();
                    try {
                        sharedContext.close();
                    } catch (Exception e) {
                        LOG.debug("Error closing failed shared context", e);
                    }
                }
                if (wrapJmsContext) {
                    try {
                        sharedContext = JmsContextWrapper.connectionOnly(delegate.createConnection());
                    } catch (JMSException e) {
                        throw new JMSRuntimeException(e.getMessage(), e.getErrorCode(), e);
                    }
                } else {
                    sharedContext = delegate.createContext();
                }
                if (reconnectOnException) {
                    sharedContext.setExceptionListener(this);
                }
                connectionFailed.set(false);
                LOG.debug("Created shared JMS connection");
            }
            return sharedContext;
        }
    }

    private void drainAllPools() {
        pools.values().forEach(SessionModePool::drain);
    }

    @Override
    public void onException(JMSException exception) {
        if (reconnectOnException) {
            LOG.warnf("JMS connection exception, will reconnect on next operation: %s", exception.getMessage());
            connectionFailed.set(true);
        }
    }

    public void resetConnection() {
        synchronized (connectionLock) {
            drainAllPools();
            closeShared();
            LOG.info("JMS connection reset — will reconnect on next operation");
        }
    }

    public void close() {
        synchronized (connectionLock) {
            drainAllPools();
            closeShared();
        }
    }

    private void closeShared() {
        if (sharedContext != null) {
            try {
                sharedContext.close();
            } catch (Exception e) {
                LOG.debug("Error closing shared context", e);
            }
            sharedContext = null;
        }
    }

    // --- Per-mode bounded session pool ---

    private record PooledEntry(PooledJMSContext context, long returnedAtNanos) {
    }

    private class SessionModePool {

        private final int sessionMode;
        private final Semaphore permits;
        private final ConcurrentLinkedDeque<PooledEntry> idle = new ConcurrentLinkedDeque<>();

        SessionModePool(int sessionMode, int maxSessions) {
            this.sessionMode = sessionMode;
            this.permits = new Semaphore(maxSessions);
        }

        PooledJMSContext borrow() {
            try {
                if (borrowTimeoutNanos < 0) {
                    permits.acquire();
                } else if (!permits.tryAcquire(borrowTimeoutNanos, TimeUnit.NANOSECONDS)) {
                    poolListener.onBorrowTimeout(factoryId);
                    throw new JMSRuntimeException(
                            "Session pool exhausted (max " + maxSessions + " per mode, timeout "
                                    + Duration.ofNanos(borrowTimeoutNanos) + ")");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new JMSRuntimeException("Interrupted waiting for pooled session");
            }

            PooledEntry entry;
            while ((entry = idle.pollFirst()) != null) {
                try {
                    entry.context.getSessionMode();
                    poolListener.onSessionBorrowed(factoryId);
                    return entry.context;
                } catch (Exception e) {
                    LOG.debug("Evicting stale pooled session", e);
                    entry.context.closeAll();
                    poolListener.onSessionClosed(factoryId);
                }
            }

            JMSContext root = getSharedContext();
            JMSContext child = root.createContext(sessionMode);
            poolListener.onSessionCreated(factoryId);
            poolListener.onSessionBorrowed(factoryId);
            return new PooledJMSContext(child, this::returnSession, cacheConsumers);
        }

        void returnSession(PooledJMSContext ctx) {
            if (connectionFailed.get()) {
                ctx.closeAll();
                permits.release();
                poolListener.onSessionReturned(factoryId);
                poolListener.onSessionClosed(factoryId);
                return;
            }

            idle.offerFirst(new PooledEntry(ctx, System.nanoTime()));
            trimIdle();
            evictExpired();
            permits.release();
            poolListener.onSessionReturned(factoryId);
        }

        private void trimIdle() {
            while (idle.size() > maxIdleSessions) {
                PooledEntry evicted = idle.pollLast();
                if (evicted != null) {
                    evicted.context.closeAll();
                    poolListener.onSessionClosed(factoryId);
                }
            }
        }

        private void evictExpired() {
            long now = System.nanoTime();
            PooledEntry tail;
            while ((tail = idle.peekLast()) != null && (now - tail.returnedAtNanos) > idleTimeoutNanos) {
                PooledEntry removed = idle.pollLast();
                if (removed != null) {
                    removed.context.closeAll();
                    poolListener.onSessionClosed(factoryId);
                }
            }
        }

        void drain() {
            PooledEntry entry;
            while ((entry = idle.pollFirst()) != null) {
                entry.context.closeAll();
                poolListener.onSessionClosed(factoryId);
            }
        }
    }
}
