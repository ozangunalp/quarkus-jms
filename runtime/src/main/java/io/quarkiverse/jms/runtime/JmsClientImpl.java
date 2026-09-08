package io.quarkiverse.jms.runtime;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.DeliveryMode;
import jakarta.jms.Destination;
import jakarta.jms.JMSContext;
import jakarta.transaction.Synchronization;
import jakarta.transaction.TransactionManager;

import org.jboss.logging.Logger;

import io.quarkiverse.jms.JmsClient;
import io.quarkiverse.jms.JmsDestination;
import io.quarkiverse.jms.JmsQueue;
import io.quarkiverse.jms.JmsTopic;

@ApplicationScoped
public class JmsClientImpl implements JmsClient {

    private static final Logger LOG = Logger.getLogger(JmsClientImpl.class);

    final JmsConnectionFactoryResolver connectionFactoryResolver;
    final JmsMessageConverterResolver converterResolver;
    final Instance<TransactionManager> transactionManagerInstance;
    final JmsInstrumentation instrumentation;
    final SessionPoolListener poolListener;
    final Duration defaultReceiveTimeout;
    final long defaultDeliveryDelay;
    final int defaultDeliveryMode;
    final int defaultPriority;
    final long defaultTimeToLive;

    final boolean poolEnabled;
    final int poolMaxSessions;
    final int poolMaxIdleSessions;
    final Duration poolBorrowTimeout; // null = infinite
    final Duration poolIdleTimeout;
    final boolean poolCacheConsumers;
    final boolean poolReconnectOnException;
    final boolean wrapJmsContext;
    final Map<String, SessionPoolingConnectionFactory> poolingFactories = new ConcurrentHashMap<>();
    private final Map<String, Destination> destinationCache = new ConcurrentHashMap<>();

    volatile boolean externalPoolingPresent = false;

    @Inject
    JmsClientImpl(JmsConnectionFactoryResolver connectionFactoryResolver,
            JmsMessageConverterResolver converterResolver,
            Instance<TransactionManager> transactionManagerInstance,
            JmsRuntimeConfig runtimeConfig,
            Instance<JmsInstrumentation> instrumentations,
            Instance<SessionPoolListener> poolListeners) {
        this.connectionFactoryResolver = connectionFactoryResolver;
        this.converterResolver = converterResolver;
        this.transactionManagerInstance = transactionManagerInstance;
        this.poolEnabled = runtimeConfig.pool().enabled();
        this.poolMaxSessions = runtimeConfig.pool().maxSessions();
        this.poolMaxIdleSessions = runtimeConfig.pool().maxIdleSessions();
        this.poolBorrowTimeout = runtimeConfig.pool().borrowTimeout().orElse(null);
        this.poolIdleTimeout = runtimeConfig.pool().idleTimeout();
        this.poolCacheConsumers = runtimeConfig.pool().cacheConsumers();
        this.poolReconnectOnException = runtimeConfig.pool().reconnectOnException();
        this.wrapJmsContext = runtimeConfig.jms11();
        this.instrumentation = JmsInstrumentation.create(instrumentations.stream().toList());
        this.poolListener = poolListeners.stream().findFirst().orElse(SessionPoolListener.NOOP);
        this.defaultReceiveTimeout = runtimeConfig.receiveTimeout();
        this.defaultDeliveryDelay = runtimeConfig.client().deliveryDelay().orElse(-1L);
        this.defaultDeliveryMode = parseDeliveryMode(runtimeConfig.client().deliveryMode());
        this.defaultPriority = runtimeConfig.client().priority();
        this.defaultTimeToLive = runtimeConfig.client().timeToLive();
    }

    public void setExternalPoolingPresent(boolean present) {
        this.externalPoolingPresent = present;
    }

    @Override
    public void resetConnection() {
        poolingFactories.forEach((key, factory) -> factory.resetConnection());
        destinationCache.clear();
    }

    @PreDestroy
    void closePoolingFactories() {
        poolingFactories.forEach((key, factory) -> factory.close());
        poolingFactories.clear();
        destinationCache.clear();
    }

    @Override
    public <T> JmsQueue<T> queue(String name, Class<T> type) {
        return new JmsQueueImpl<>(this, name, type, null, null);
    }

    @Override
    public JmsQueue<Object> queue(String name) {
        return new JmsQueueImpl<>(this, name, Object.class, null, null);
    }

    @Override
    public <T> JmsTopic<T> topic(String name, Class<T> type) {
        return new JmsTopicImpl<>(this, name, type, null, null);
    }

    @Override
    public JmsTopic<Object> topic(String name) {
        return new JmsTopicImpl<>(this, name, Object.class, null, null);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public JmsDestination createConfigured(String name, boolean isTopic, Class<?> messageType,
            JmsDestinationConfig config) {
        String cfId = config.connectionFactory().orElse(null);
        String selector = config.selector().orElse(null);
        long ttl = config.ttl().map(s -> java.time.Duration.parse(s).toMillis()).orElse(defaultTimeToLive);
        long delay = config.deliveryDelay().map(s -> java.time.Duration.parse(s).toMillis()).orElse(defaultDeliveryDelay);
        int mode = config.deliveryMode().map(JmsClientImpl::parseDeliveryMode)
                .filter(v -> v >= 0).orElse(defaultDeliveryMode);
        int prio = config.priority().orElse(defaultPriority);
        return isTopic ? new JmsTopicImpl(this, name, messageType, cfId, selector, ttl, delay, mode, prio)
                : new JmsQueueImpl(this, name, messageType, cfId, selector, ttl, delay, mode, prio);
    }

    private static int parseDeliveryMode(String mode) {
        return switch (mode.toLowerCase()) {
            case "persistent" -> DeliveryMode.PERSISTENT;
            case "non_persistent", "non-persistent" -> DeliveryMode.NON_PERSISTENT;
            default -> -1;
        };
    }

    Destination getOrCreateDestination(String name, boolean isTopic, JMSContext context) {
        return destinationCache.computeIfAbsent(
                (isTopic ? "topic:" : "queue:") + name,
                k -> isTopic ? context.createTopic(name) : context.createQueue(name));
    }

    ConnectionFactory resolveFactory(String id) {
        if (externalPoolingPresent || !poolEnabled) {
            ConnectionFactory cf = connectionFactoryResolver.resolve(id);
            return wrapJmsContext ? new JmsContextWrappingConnectionFactory(cf) : cf;
        }
        String key = id != null ? id : "__default__";
        return poolingFactories.computeIfAbsent(key, k -> new SessionPoolingConnectionFactory(
                connectionFactoryResolver.resolve(id),
                poolMaxSessions, poolMaxIdleSessions, poolBorrowTimeout, poolIdleTimeout,
                poolCacheConsumers, poolReconnectOnException, wrapJmsContext,
                key, poolListener));
    }

    // --- Synchronization records ---

    record ManagedContext(JMSContext context, boolean transacted) implements AutoCloseable {
        void releaseIfNotTransacted() {
            if (!transacted) {
                context.close();
            }
        }

        @Override
        public void close() {
            releaseIfNotTransacted();
        }
    }

    record ContextCloser(JMSContext context) implements Synchronization {
        @Override
        public void beforeCompletion() {
        }

        @Override
        public void afterCompletion(int status) {
            try {
                context.close();
            } catch (Exception e) {
                LOG.debug("Error closing JMS context after transaction", e);
            }
        }
    }

    record TransactedContextSynchronization(JMSContext context) implements Synchronization {
        @Override
        public void beforeCompletion() {
        }

        @Override
        public void afterCompletion(int status) {
            try {
                if (status == jakarta.transaction.Status.STATUS_COMMITTED) {
                    context.commit();
                } else {
                    context.rollback();
                }
            } catch (Exception e) {
                LOG.debug("Error completing JMS session transaction", e);
            } finally {
                try {
                    context.close();
                } catch (Exception e) {
                    LOG.debug("Error closing JMS context after transaction", e);
                }
            }
        }
    }
}
