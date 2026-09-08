package io.quarkiverse.jms.runtime;

import java.io.Serializable;
import java.util.Deque;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.function.Consumer;

import jakarta.jms.BytesMessage;
import jakarta.jms.ConnectionMetaData;
import jakarta.jms.Destination;
import jakarta.jms.ExceptionListener;
import jakarta.jms.JMSConsumer;
import jakarta.jms.JMSContext;
import jakarta.jms.JMSException;
import jakarta.jms.JMSProducer;
import jakarta.jms.MapMessage;
import jakarta.jms.Message;
import jakarta.jms.ObjectMessage;
import jakarta.jms.Queue;
import jakarta.jms.QueueBrowser;
import jakarta.jms.StreamMessage;
import jakarta.jms.TemporaryQueue;
import jakarta.jms.TemporaryTopic;
import jakarta.jms.TextMessage;
import jakarta.jms.Topic;

import org.jboss.logging.Logger;

/**
 * A pooled {@link JMSContext} that caches consumers, tracks temporary
 * destinations, and returns itself to the pool on {@link #close()}.
 * Survives pool round-trips — caches are preserved when the context
 * is returned to the pool and borrowed again.
 */
class PooledJMSContext implements JMSContext {

    private static final Logger LOG = Logger.getLogger(PooledJMSContext.class);

    private final JMSContext delegate;
    private final boolean cacheConsumers;
    private final Consumer<PooledJMSContext> onClose;
    private final Map<ConsumerKey, JMSConsumer> consumerCache = new ConcurrentHashMap<>();
    private final Deque<JMSConsumer> activeConsumers = new ConcurrentLinkedDeque<>();
    private final Deque<TemporaryQueue> tempQueues = new ConcurrentLinkedDeque<>();
    private final Deque<TemporaryTopic> tempTopics = new ConcurrentLinkedDeque<>();

    PooledJMSContext(JMSContext delegate, Consumer<PooledJMSContext> onClose, boolean cacheConsumers) {
        this.delegate = delegate;
        this.cacheConsumers = cacheConsumers;
        this.onClose = onClose;
    }

    @Override
    public void close() {
        closeActiveConsumers();
        cleanupTempDestinations();
        onClose.accept(this);
    }

    private void closeActiveConsumers() {
        JMSConsumer c;
        while ((c = activeConsumers.pollFirst()) != null) {
            try {
                c.close();
            } catch (Exception e) {
                LOG.debug("Error closing active consumer", e);
            }
        }
    }

    private void clearConsumerCache() {
        consumerCache.values().forEach(c -> {
            try {
                c.close();
            } catch (Exception e) {
                LOG.debug("Error closing cached consumer", e);
            }
        });
        consumerCache.clear();
    }

    @Override
    public JMSContext createContext(int sessionMode) {
        return delegate.createContext(sessionMode);
    }

    @Override
    public JMSProducer createProducer() {
        return delegate.createProducer();
    }

    @Override
    public void start() {
        // no-op — shared connection lifecycle managed by factory
    }

    @Override
    public void stop() {
        // no-op — shared connection lifecycle managed by factory
    }

    // --- Consumer caching ---

    @Override
    public JMSConsumer createConsumer(Destination destination) {
        return createConsumer(destination, null, false);
    }

    @Override
    public JMSConsumer createConsumer(Destination destination, String messageSelector) {
        return createConsumer(destination, messageSelector, false);
    }

    @Override
    public JMSConsumer createConsumer(Destination destination, String messageSelector, boolean noLocal) {
        if (!cacheConsumers) {
            return trackConsumer(delegate.createConsumer(destination, messageSelector, noLocal));
        }
        ConsumerKey key = ConsumerKey.of(destination, messageSelector, noLocal);
        return consumerCache.computeIfAbsent(key,
                k -> delegate.createConsumer(destination, messageSelector, noLocal));
    }

    @Override
    public JMSConsumer createDurableConsumer(Topic topic, String name) {
        return trackConsumer(delegate.createDurableConsumer(topic, name));
    }

    @Override
    public JMSConsumer createDurableConsumer(Topic topic, String name, String messageSelector, boolean noLocal) {
        return trackConsumer(delegate.createDurableConsumer(topic, name, messageSelector, noLocal));
    }

    @Override
    public JMSConsumer createSharedDurableConsumer(Topic topic, String name) {
        return trackConsumer(delegate.createSharedDurableConsumer(topic, name));
    }

    @Override
    public JMSConsumer createSharedDurableConsumer(Topic topic, String name, String messageSelector) {
        return trackConsumer(delegate.createSharedDurableConsumer(topic, name, messageSelector));
    }

    @Override
    public JMSConsumer createSharedConsumer(Topic topic, String sharedSubscriptionName) {
        return trackConsumer(delegate.createSharedConsumer(topic, sharedSubscriptionName));
    }

    @Override
    public JMSConsumer createSharedConsumer(Topic topic, String sharedSubscriptionName, String messageSelector) {
        return trackConsumer(delegate.createSharedConsumer(topic, sharedSubscriptionName, messageSelector));
    }

    private JMSConsumer trackConsumer(JMSConsumer consumer) {
        activeConsumers.add(consumer);
        return consumer;
    }

    // --- Temporary destination tracking ---

    @Override
    public TemporaryQueue createTemporaryQueue() {
        TemporaryQueue tq = delegate.createTemporaryQueue();
        tempQueues.add(tq);
        return tq;
    }

    @Override
    public TemporaryTopic createTemporaryTopic() {
        TemporaryTopic tt = delegate.createTemporaryTopic();
        tempTopics.add(tt);
        return tt;
    }

    // --- Lifecycle ---

    void cleanupTempDestinations() {
        TemporaryQueue tq;
        while ((tq = tempQueues.pollFirst()) != null) {
            try {
                tq.delete();
            } catch (Exception e) {
                LOG.debug("Error deleting temporary queue", e);
            }
        }
        TemporaryTopic tt;
        while ((tt = tempTopics.pollFirst()) != null) {
            try {
                tt.delete();
            } catch (Exception e) {
                LOG.debug("Error deleting temporary topic", e);
            }
        }
    }

    void closeAll() {
        clearConsumerCache();
        cleanupTempDestinations();
        try {
            delegate.close();
        } catch (Exception e) {
            LOG.debug("Error closing cached session", e);
        }
    }

    // --- Delegating methods ---

    @Override
    public String getClientID() {
        return delegate.getClientID();
    }

    @Override
    public void setClientID(String clientID) {
        delegate.setClientID(clientID);
    }

    @Override
    public ConnectionMetaData getMetaData() {
        return delegate.getMetaData();
    }

    @Override
    public ExceptionListener getExceptionListener() {
        return delegate.getExceptionListener();
    }

    @Override
    public void setExceptionListener(ExceptionListener listener) {
        delegate.setExceptionListener(listener);
    }

    @Override
    public void setAutoStart(boolean autoStart) {
        delegate.setAutoStart(autoStart);
    }

    @Override
    public boolean getAutoStart() {
        return delegate.getAutoStart();
    }

    @Override
    public BytesMessage createBytesMessage() {
        return delegate.createBytesMessage();
    }

    @Override
    public MapMessage createMapMessage() {
        return delegate.createMapMessage();
    }

    @Override
    public Message createMessage() {
        return delegate.createMessage();
    }

    @Override
    public ObjectMessage createObjectMessage() {
        return delegate.createObjectMessage();
    }

    @Override
    public ObjectMessage createObjectMessage(Serializable object) {
        return delegate.createObjectMessage(object);
    }

    @Override
    public StreamMessage createStreamMessage() {
        return delegate.createStreamMessage();
    }

    @Override
    public TextMessage createTextMessage() {
        return delegate.createTextMessage();
    }

    @Override
    public TextMessage createTextMessage(String text) {
        return delegate.createTextMessage(text);
    }

    @Override
    public boolean getTransacted() {
        return delegate.getTransacted();
    }

    @Override
    public int getSessionMode() {
        return delegate.getSessionMode();
    }

    @Override
    public void commit() {
        delegate.commit();
    }

    @Override
    public void rollback() {
        delegate.rollback();
    }

    @Override
    public void recover() {
        delegate.recover();
    }

    @Override
    public Queue createQueue(String queueName) {
        return delegate.createQueue(queueName);
    }

    @Override
    public Topic createTopic(String topicName) {
        return delegate.createTopic(topicName);
    }

    @Override
    public QueueBrowser createBrowser(Queue queue) {
        return delegate.createBrowser(queue);
    }

    @Override
    public QueueBrowser createBrowser(Queue queue, String messageSelector) {
        return delegate.createBrowser(queue, messageSelector);
    }

    @Override
    public void unsubscribe(String name) {
        delegate.unsubscribe(name);
    }

    @Override
    public void acknowledge() {
        delegate.acknowledge();
    }

    record ConsumerKey(String destinationName, boolean isTopic, String selector, boolean noLocal) {

        static ConsumerKey of(Destination destination, String selector, boolean noLocal) {
            try {
                if (destination instanceof Topic topic) {
                    return new ConsumerKey(topic.getTopicName(), true, selector, noLocal);
                } else if (destination instanceof Queue queue) {
                    return new ConsumerKey(queue.getQueueName(), false, selector, noLocal);
                }
            } catch (JMSException e) {
                // fall through
            }
            return new ConsumerKey(destination.toString(), false, selector, noLocal);
        }
    }
}
