package io.quarkiverse.jms.runtime;

import java.io.Serializable;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import jakarta.jms.CompletionListener;
import jakarta.jms.DeliveryMode;
import jakarta.jms.Destination;
import jakarta.jms.JMSException;
import jakarta.jms.JMSProducer;
import jakarta.jms.JMSRuntimeException;
import jakarta.jms.Message;
import jakarta.jms.MessageProducer;
import jakarta.jms.Session;

/**
 * A {@link JMSProducer} implementation wrapping a JMS 1.1 {@link MessageProducer}.
 */
class JmsProducerWrapper implements JMSProducer, AutoCloseable {

    private final Session session;
    private final MessageProducer producer;
    private final Map<String, Object> properties = new ConcurrentHashMap<>();
    private int deliveryMode = DeliveryMode.PERSISTENT;
    private int priority = Message.DEFAULT_PRIORITY;
    private long timeToLive = Message.DEFAULT_TIME_TO_LIVE;
    private long deliveryDelay = Message.DEFAULT_DELIVERY_DELAY;
    private String correlationId;
    private Destination replyTo;
    private String type;

    JmsProducerWrapper(Session session, MessageProducer producer) {
        this.session = session;
        this.producer = producer;
    }

    @Override
    public JMSProducer send(Destination destination, Message message) {
        try {
            applyProperties(message);
            if (correlationId != null) {
                message.setJMSCorrelationID(correlationId);
            }
            if (replyTo != null) {
                message.setJMSReplyTo(replyTo);
            }
            if (type != null) {
                message.setJMSType(type);
            }
            producer.send(destination, message, deliveryMode, priority, timeToLive);
        } catch (JMSException e) {
            throw toRuntime(e);
        }
        return this;
    }

    @Override
    public JMSProducer send(Destination destination, String body) {
        try {
            return send(destination, session.createTextMessage(body));
        } catch (JMSException e) {
            throw toRuntime(e);
        }
    }

    @Override
    public JMSProducer send(Destination destination, Map<String, Object> body) {
        try {
            var msg = session.createMapMessage();
            if (body != null) {
                for (var entry : body.entrySet()) {
                    msg.setObject(entry.getKey(), entry.getValue());
                }
            }
            return send(destination, (Message) msg);
        } catch (JMSException e) {
            throw toRuntime(e);
        }
    }

    @Override
    public JMSProducer send(Destination destination, byte[] body) {
        try {
            var msg = session.createBytesMessage();
            if (body != null) {
                msg.writeBytes(body);
            }
            return send(destination, (Message) msg);
        } catch (JMSException e) {
            throw toRuntime(e);
        }
    }

    @Override
    public JMSProducer send(Destination destination, Serializable body) {
        try {
            return send(destination, session.createObjectMessage(body));
        } catch (JMSException e) {
            throw toRuntime(e);
        }
    }

    private void applyProperties(Message message) throws JMSException {
        for (var entry : properties.entrySet()) {
            message.setObjectProperty(entry.getKey(), entry.getValue());
        }
    }

    @Override
    public JMSProducer setDisableMessageID(boolean value) {
        return this;
    }

    @Override
    public boolean getDisableMessageID() {
        return false;
    }

    @Override
    public JMSProducer setDisableMessageTimestamp(boolean value) {
        return this;
    }

    @Override
    public boolean getDisableMessageTimestamp() {
        return false;
    }

    @Override
    public JMSProducer setDeliveryMode(int deliveryMode) {
        this.deliveryMode = deliveryMode;
        return this;
    }

    @Override
    public int getDeliveryMode() {
        return deliveryMode;
    }

    @Override
    public JMSProducer setPriority(int priority) {
        this.priority = priority;
        return this;
    }

    @Override
    public int getPriority() {
        return priority;
    }

    @Override
    public JMSProducer setTimeToLive(long timeToLive) {
        this.timeToLive = timeToLive;
        return this;
    }

    @Override
    public long getTimeToLive() {
        return timeToLive;
    }

    @Override
    public JMSProducer setDeliveryDelay(long deliveryDelay) {
        this.deliveryDelay = deliveryDelay;
        return this;
    }

    @Override
    public long getDeliveryDelay() {
        return deliveryDelay;
    }

    @Override
    public JMSProducer setAsync(CompletionListener completionListener) {
        throw new JMSRuntimeException("Async send not supported");
    }

    @Override
    public CompletionListener getAsync() {
        return null;
    }

    @Override
    public JMSProducer setProperty(String name, boolean value) {
        properties.put(name, value);
        return this;
    }

    @Override
    public JMSProducer setProperty(String name, byte value) {
        properties.put(name, value);
        return this;
    }

    @Override
    public JMSProducer setProperty(String name, short value) {
        properties.put(name, value);
        return this;
    }

    @Override
    public JMSProducer setProperty(String name, int value) {
        properties.put(name, value);
        return this;
    }

    @Override
    public JMSProducer setProperty(String name, long value) {
        properties.put(name, value);
        return this;
    }

    @Override
    public JMSProducer setProperty(String name, float value) {
        properties.put(name, value);
        return this;
    }

    @Override
    public JMSProducer setProperty(String name, double value) {
        properties.put(name, value);
        return this;
    }

    @Override
    public JMSProducer setProperty(String name, String value) {
        properties.put(name, value);
        return this;
    }

    @Override
    public JMSProducer setProperty(String name, Object value) {
        properties.put(name, value);
        return this;
    }

    @Override
    public JMSProducer clearProperties() {
        properties.clear();
        return this;
    }

    @Override
    public boolean propertyExists(String name) {
        return properties.containsKey(name);
    }

    @Override
    public boolean getBooleanProperty(String name) {
        return (boolean) properties.get(name);
    }

    @Override
    public byte getByteProperty(String name) {
        return (byte) properties.get(name);
    }

    @Override
    public short getShortProperty(String name) {
        return (short) properties.get(name);
    }

    @Override
    public int getIntProperty(String name) {
        return (int) properties.get(name);
    }

    @Override
    public long getLongProperty(String name) {
        return (long) properties.get(name);
    }

    @Override
    public float getFloatProperty(String name) {
        return (float) properties.get(name);
    }

    @Override
    public double getDoubleProperty(String name) {
        return (double) properties.get(name);
    }

    @Override
    public String getStringProperty(String name) {
        return (String) properties.get(name);
    }

    @Override
    public Object getObjectProperty(String name) {
        return properties.get(name);
    }

    @Override
    public Set<String> getPropertyNames() {
        return properties.keySet();
    }

    @Override
    public JMSProducer setJMSCorrelationIDAsBytes(byte[] correlationID) {
        return this;
    }

    @Override
    public byte[] getJMSCorrelationIDAsBytes() {
        return null;
    }

    @Override
    public JMSProducer setJMSCorrelationID(String correlationID) {
        this.correlationId = correlationID;
        return this;
    }

    @Override
    public String getJMSCorrelationID() {
        return correlationId;
    }

    @Override
    public JMSProducer setJMSType(String type) {
        this.type = type;
        return this;
    }

    @Override
    public String getJMSType() {
        return type;
    }

    @Override
    public JMSProducer setJMSReplyTo(Destination replyTo) {
        this.replyTo = replyTo;
        return this;
    }

    @Override
    public Destination getJMSReplyTo() {
        return replyTo;
    }

    @Override
    public void close() throws JMSException {
        producer.close();
    }

    private static JMSRuntimeException toRuntime(JMSException e) {
        return new JMSRuntimeException(e.getMessage(), e.getErrorCode(), e);
    }
}
