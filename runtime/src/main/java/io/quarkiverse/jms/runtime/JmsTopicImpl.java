package io.quarkiverse.jms.runtime;

import io.quarkiverse.jms.JmsTopic;

class JmsTopicImpl<T> extends JmsDestinationImpl<T> implements JmsTopic<T> {

    JmsTopicImpl(JmsClientImpl client, String name, Class<T> messageType,
            String connectionFactoryId, String defaultSelector) {
        super(client, name, true, messageType, connectionFactoryId, defaultSelector);
    }

    JmsTopicImpl(JmsClientImpl client, String name, Class<T> messageType,
            String connectionFactoryId, String defaultSelector,
            long ttl, long deliveryDelay, int deliveryMode, int priority) {
        super(client, name, true, messageType, connectionFactoryId, defaultSelector,
                ttl, deliveryDelay, deliveryMode, priority);
    }
}
