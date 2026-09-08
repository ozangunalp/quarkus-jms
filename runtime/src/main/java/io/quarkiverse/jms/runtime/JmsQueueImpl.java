package io.quarkiverse.jms.runtime;

import java.util.List;

import io.quarkiverse.jms.JmsQueue;
import io.quarkiverse.jms.ReceiveMessage;

class JmsQueueImpl<T> extends JmsDestinationImpl<T> implements JmsQueue<T> {

    JmsQueueImpl(JmsClientImpl client, String name, Class<T> messageType,
            String connectionFactoryId, String defaultSelector) {
        super(client, name, false, messageType, connectionFactoryId, defaultSelector);
    }

    JmsQueueImpl(JmsClientImpl client, String name, Class<T> messageType,
            String connectionFactoryId, String defaultSelector,
            long ttl, long deliveryDelay, int deliveryMode, int priority) {
        super(client, name, false, messageType, connectionFactoryId, defaultSelector,
                ttl, deliveryDelay, deliveryMode, priority);
    }

    @Override
    public List<T> browse() {
        return doBrowse(defaultSelector);
    }

    @Override
    public List<T> browse(String selector) {
        return doBrowse(selector);
    }

    @Override
    public List<ReceiveMessage<T>> browseMessages() {
        return doBrowseMessages(defaultSelector);
    }

    @Override
    public List<ReceiveMessage<T>> browseMessages(String selector) {
        return doBrowseMessages(selector);
    }
}
