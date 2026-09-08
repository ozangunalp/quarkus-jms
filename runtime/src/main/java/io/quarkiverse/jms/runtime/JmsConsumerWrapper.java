package io.quarkiverse.jms.runtime;

import jakarta.jms.JMSConsumer;
import jakarta.jms.JMSException;
import jakarta.jms.JMSRuntimeException;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageListener;

/**
 * A {@link JMSConsumer} implementation wrapping a JMS 1.1 {@link MessageConsumer}.
 */
class JmsConsumerWrapper implements JMSConsumer {

    private final MessageConsumer consumer;

    JmsConsumerWrapper(MessageConsumer consumer) {
        this.consumer = consumer;
    }

    @Override
    public String getMessageSelector() {
        try {
            return consumer.getMessageSelector();
        } catch (JMSException e) {
            throw toRuntime(e);
        }
    }

    @Override
    public MessageListener getMessageListener() throws JMSRuntimeException {
        try {
            return consumer.getMessageListener();
        } catch (JMSException e) {
            throw toRuntime(e);
        }
    }

    @Override
    public void setMessageListener(MessageListener listener) throws JMSRuntimeException {
        try {
            consumer.setMessageListener(listener);
        } catch (JMSException e) {
            throw toRuntime(e);
        }
    }

    @Override
    public Message receive() {
        try {
            return consumer.receive();
        } catch (JMSException e) {
            throw toRuntime(e);
        }
    }

    @Override
    public Message receive(long timeout) {
        try {
            return consumer.receive(timeout);
        } catch (JMSException e) {
            throw toRuntime(e);
        }
    }

    @Override
    public Message receiveNoWait() {
        try {
            return consumer.receiveNoWait();
        } catch (JMSException e) {
            throw toRuntime(e);
        }
    }

    @Override
    public <T> T receiveBody(Class<T> c) {
        try {
            Message msg = consumer.receive();
            return msg != null ? msg.getBody(c) : null;
        } catch (JMSException e) {
            throw toRuntime(e);
        }
    }

    @Override
    public <T> T receiveBody(Class<T> c, long timeout) {
        try {
            Message msg = consumer.receive(timeout);
            return msg != null ? msg.getBody(c) : null;
        } catch (JMSException e) {
            throw toRuntime(e);
        }
    }

    @Override
    public <T> T receiveBodyNoWait(Class<T> c) {
        try {
            Message msg = consumer.receiveNoWait();
            return msg != null ? msg.getBody(c) : null;
        } catch (JMSException e) {
            throw toRuntime(e);
        }
    }

    @Override
    public void close() {
        try {
            consumer.close();
        } catch (JMSException e) {
            throw toRuntime(e);
        }
    }

    private static JMSRuntimeException toRuntime(JMSException e) {
        return new JMSRuntimeException(e.getMessage(), e.getErrorCode(), e);
    }
}
