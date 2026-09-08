package io.quarkiverse.jms.runtime;

import java.io.Serializable;

import jakarta.jms.BytesMessage;
import jakarta.jms.Connection;
import jakarta.jms.ConnectionMetaData;
import jakarta.jms.Destination;
import jakarta.jms.ExceptionListener;
import jakarta.jms.JMSConsumer;
import jakarta.jms.JMSContext;
import jakarta.jms.JMSException;
import jakarta.jms.JMSProducer;
import jakarta.jms.JMSRuntimeException;
import jakarta.jms.MapMessage;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageProducer;
import jakarta.jms.ObjectMessage;
import jakarta.jms.Queue;
import jakarta.jms.QueueBrowser;
import jakarta.jms.Session;
import jakarta.jms.StreamMessage;
import jakarta.jms.TemporaryQueue;
import jakarta.jms.TemporaryTopic;
import jakarta.jms.TextMessage;
import jakarta.jms.Topic;

/**
 * A {@link JMSContext} implementation that wraps a JMS 1.1 {@link Connection}
 * and {@link Session}. Enables the Quarkus JMS extension to work with providers
 * that only support JMS 1.1 (e.g., Amazon SQS, Oracle AQ).
 */
class JmsContextWrapper implements JMSContext {

    final Connection connection;
    private final Session session;
    private final int sessionMode;
    private final boolean ownsConnection;
    private volatile MessageProducer anonymousProducer;
    private boolean autoStart = true;

    JmsContextWrapper(Connection connection, int sessionMode) {
        this(connection, sessionMode, true);
    }

    JmsContextWrapper(Connection connection, int sessionMode, boolean ownsConnection) {
        this.connection = connection;
        this.sessionMode = sessionMode;
        this.ownsConnection = ownsConnection;
        try {
            this.session = connection.createSession(sessionMode);
        } catch (JMSException e) {
            throw toRuntime(e);
        }
    }

    private JmsContextWrapper(Connection connection, boolean ownsConnection) {
        this.connection = connection;
        this.session = null;
        this.sessionMode = Session.AUTO_ACKNOWLEDGE;
        this.ownsConnection = ownsConnection;
    }

    static JmsContextWrapper connectionOnly(Connection connection) {
        return new JmsContextWrapper(connection, true);
    }

    @Override
    public JMSContext createContext(int sessionMode) {
        return new JmsContextWrapper(connection, sessionMode, false);
    }

    @Override
    public JMSProducer createProducer() {
        try {
            if (anonymousProducer == null) {
                synchronized (this) {
                    if (anonymousProducer == null) {
                        anonymousProducer = session.createProducer(null);
                    }
                }
            }
            return new JmsProducerWrapper(session, anonymousProducer);
        } catch (JMSException e) {
            throw toRuntime(e);
        }
    }

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
        try {
            startIfNeeded();
            MessageConsumer consumer = session.createConsumer(destination, messageSelector, noLocal);
            return new JmsConsumerWrapper(consumer);
        } catch (JMSException e) {
            throw toRuntime(e);
        }
    }

    @Override
    public JMSConsumer createDurableConsumer(Topic topic, String name) {
        return createDurableConsumer(topic, name, null, false);
    }

    @Override
    public JMSConsumer createDurableConsumer(Topic topic, String name, String messageSelector, boolean noLocal) {
        try {
            startIfNeeded();
            return new JmsConsumerWrapper(session.createDurableConsumer(topic, name, messageSelector, noLocal));
        } catch (JMSException e) {
            throw toRuntime(e);
        }
    }

    @Override
    public JMSConsumer createSharedDurableConsumer(Topic topic, String name) {
        return createSharedDurableConsumer(topic, name, null);
    }

    @Override
    public JMSConsumer createSharedDurableConsumer(Topic topic, String name, String messageSelector) {
        try {
            startIfNeeded();
            return new JmsConsumerWrapper(session.createSharedDurableConsumer(topic, name, messageSelector));
        } catch (JMSException e) {
            throw toRuntime(e);
        }
    }

    @Override
    public JMSConsumer createSharedConsumer(Topic topic, String sharedSubscriptionName) {
        return createSharedConsumer(topic, sharedSubscriptionName, null);
    }

    @Override
    public JMSConsumer createSharedConsumer(Topic topic, String sharedSubscriptionName, String messageSelector) {
        try {
            startIfNeeded();
            return new JmsConsumerWrapper(session.createSharedConsumer(topic, sharedSubscriptionName, messageSelector));
        } catch (JMSException e) {
            throw toRuntime(e);
        }
    }

    @Override
    public Queue createQueue(String queueName) {
        try {
            return session.createQueue(queueName);
        } catch (JMSException e) {
            throw toRuntime(e);
        }
    }

    @Override
    public Topic createTopic(String topicName) {
        try {
            return session.createTopic(topicName);
        } catch (JMSException e) {
            throw toRuntime(e);
        }
    }

    @Override
    public QueueBrowser createBrowser(Queue queue) {
        try {
            return session.createBrowser(queue);
        } catch (JMSException e) {
            throw toRuntime(e);
        }
    }

    @Override
    public QueueBrowser createBrowser(Queue queue, String messageSelector) {
        try {
            return session.createBrowser(queue, messageSelector);
        } catch (JMSException e) {
            throw toRuntime(e);
        }
    }

    @Override
    public TemporaryQueue createTemporaryQueue() {
        try {
            return session.createTemporaryQueue();
        } catch (JMSException e) {
            throw toRuntime(e);
        }
    }

    @Override
    public TemporaryTopic createTemporaryTopic() {
        try {
            return session.createTemporaryTopic();
        } catch (JMSException e) {
            throw toRuntime(e);
        }
    }

    @Override
    public void unsubscribe(String name) {
        try {
            session.unsubscribe(name);
        } catch (JMSException e) {
            throw toRuntime(e);
        }
    }

    @Override
    public BytesMessage createBytesMessage() {
        try {
            return session.createBytesMessage();
        } catch (JMSException e) {
            throw toRuntime(e);
        }
    }

    @Override
    public MapMessage createMapMessage() {
        try {
            return session.createMapMessage();
        } catch (JMSException e) {
            throw toRuntime(e);
        }
    }

    @Override
    public Message createMessage() {
        try {
            return session.createMessage();
        } catch (JMSException e) {
            throw toRuntime(e);
        }
    }

    @Override
    public ObjectMessage createObjectMessage() {
        try {
            return session.createObjectMessage();
        } catch (JMSException e) {
            throw toRuntime(e);
        }
    }

    @Override
    public ObjectMessage createObjectMessage(Serializable object) {
        try {
            return session.createObjectMessage(object);
        } catch (JMSException e) {
            throw toRuntime(e);
        }
    }

    @Override
    public StreamMessage createStreamMessage() {
        try {
            return session.createStreamMessage();
        } catch (JMSException e) {
            throw toRuntime(e);
        }
    }

    @Override
    public TextMessage createTextMessage() {
        try {
            return session.createTextMessage();
        } catch (JMSException e) {
            throw toRuntime(e);
        }
    }

    @Override
    public TextMessage createTextMessage(String text) {
        try {
            return session.createTextMessage(text);
        } catch (JMSException e) {
            throw toRuntime(e);
        }
    }

    @Override
    public boolean getTransacted() {
        try {
            return session.getTransacted();
        } catch (JMSException e) {
            throw toRuntime(e);
        }
    }

    @Override
    public int getSessionMode() {
        return sessionMode;
    }

    @Override
    public void commit() {
        try {
            session.commit();
        } catch (JMSException e) {
            throw toRuntime(e);
        }
    }

    @Override
    public void rollback() {
        try {
            session.rollback();
        } catch (JMSException e) {
            throw toRuntime(e);
        }
    }

    @Override
    public void recover() {
        try {
            session.recover();
        } catch (JMSException e) {
            throw toRuntime(e);
        }
    }

    @Override
    public void acknowledge() {
        // In JMS 1.1, acknowledgement is done on the Message, not the Session.
        // JMSContext.acknowledge() has no direct equivalent — the extension
        // calls message.acknowledge() directly in ConsumerWorker.
    }

    @Override
    public String getClientID() {
        try {
            return connection.getClientID();
        } catch (JMSException e) {
            throw toRuntime(e);
        }
    }

    @Override
    public void setClientID(String clientID) {
        try {
            connection.setClientID(clientID);
        } catch (JMSException e) {
            throw toRuntime(e);
        }
    }

    @Override
    public ConnectionMetaData getMetaData() {
        try {
            return connection.getMetaData();
        } catch (JMSException e) {
            throw toRuntime(e);
        }
    }

    @Override
    public ExceptionListener getExceptionListener() {
        try {
            return connection.getExceptionListener();
        } catch (JMSException e) {
            throw toRuntime(e);
        }
    }

    @Override
    public void setExceptionListener(ExceptionListener listener) {
        try {
            connection.setExceptionListener(listener);
        } catch (JMSException e) {
            throw toRuntime(e);
        }
    }

    @Override
    public void start() {
        try {
            connection.start();
        } catch (JMSException e) {
            throw toRuntime(e);
        }
    }

    @Override
    public void stop() {
        try {
            connection.stop();
        } catch (JMSException e) {
            throw toRuntime(e);
        }
    }

    @Override
    public void setAutoStart(boolean autoStart) {
        this.autoStart = autoStart;
    }

    @Override
    public boolean getAutoStart() {
        return autoStart;
    }

    @Override
    public void close() {
        try {
            if (session != null) {
                session.close();
            }
        } catch (JMSException e) {
            throw toRuntime(e);
        } finally {
            if (ownsConnection) {
                try {
                    connection.close();
                } catch (JMSException e) {
                    throw toRuntime(e);
                }
            }
        }
    }

    private void startIfNeeded() {
        if (autoStart) {
            start();
        }
    }

    private static JMSRuntimeException toRuntime(JMSException e) {
        return new JMSRuntimeException(e.getMessage(), e.getErrorCode(), e);
    }

}
