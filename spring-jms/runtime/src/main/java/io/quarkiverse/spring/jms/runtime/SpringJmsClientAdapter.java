package io.quarkiverse.spring.jms.runtime;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.jms.Destination;
import jakarta.jms.JMSException;
import jakarta.jms.Queue;
import jakarta.jms.Topic;

import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.support.GenericMessage;

import io.quarkiverse.jms.JmsClient;
import io.quarkiverse.jms.JmsDeliveryMode;
import io.quarkiverse.jms.JmsDestination;
import io.quarkiverse.jms.JmsException;
import io.quarkiverse.jms.JmsReceiveException;
import io.quarkiverse.jms.JmsSendBuilder;
import io.quarkiverse.jms.ReceiveMessage;

/**
 * Adapts Quarkus {@link JmsClient} to Spring's {@link org.springframework.jms.core.JmsClient} interface.
 */
@ApplicationScoped
public class SpringJmsClientAdapter implements org.springframework.jms.core.JmsClient {

    @Inject
    JmsClient delegate;

    private volatile boolean pubSubDomain = false;

    public void setPubSubDomain(boolean pubSubDomain) {
        this.pubSubDomain = pubSubDomain;
    }

    @Override
    public OperationSpec destination(String destinationName) {
        JmsDestination<Object> dest = pubSubDomain
                ? delegate.topic(destinationName)
                : delegate.queue(destinationName);
        return new QuarkusOperationSpec(dest);
    }

    @Override
    public OperationSpec destination(Destination destination) {
        try {
            if (destination instanceof Queue queue) {
                return new QuarkusOperationSpec(delegate.queue(queue.getQueueName()));
            } else if (destination instanceof Topic topic) {
                return new QuarkusOperationSpec(delegate.topic(topic.getTopicName()));
            }
        } catch (JMSException e) {
            throw new JmsException("Failed to resolve destination name", e);
        }
        throw new IllegalArgumentException("Unsupported destination type: " + destination.getClass());
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private static class QuarkusOperationSpec implements OperationSpec {

        private final JmsDestination<Object> dest;
        private Duration receiveTimeout;
        private long deliveryDelay = -1;
        private JmsDeliveryMode deliveryMode;
        private int priority = -1;
        private long timeToLive = -1;

        QuarkusOperationSpec(JmsDestination dest) {
            this.dest = dest;
        }

        @Override
        public OperationSpec withReceiveTimeout(long receiveTimeout) {
            this.receiveTimeout = Duration.ofMillis(receiveTimeout);
            return this;
        }

        @Override
        public OperationSpec withDeliveryDelay(long deliveryDelay) {
            this.deliveryDelay = deliveryDelay;
            return this;
        }

        @Override
        public OperationSpec withDeliveryPersistent(boolean persistent) {
            this.deliveryMode = persistent ? JmsDeliveryMode.PERSISTENT : JmsDeliveryMode.NON_PERSISTENT;
            return this;
        }

        @Override
        public OperationSpec withPriority(int priority) {
            this.priority = priority;
            return this;
        }

        @Override
        public OperationSpec withTimeToLive(long timeToLive) {
            this.timeToLive = timeToLive;
            return this;
        }

        // --- Send ---

        @Override
        public void send(Message<?> message) {
            JmsSendBuilder<Object> builder = createBuilder(message.getPayload());
            for (Map.Entry<String, Object> entry : message.getHeaders().entrySet()) {
                if (!isInternalHeader(entry.getKey())) {
                    builder.property(entry.getKey(), entry.getValue());
                }
            }
            builder.send();
        }

        @Override
        public void send(Object payload) {
            createBuilder(payload).send();
        }

        @Override
        public void send(Object payload, Map<String, Object> headers) {
            JmsSendBuilder<Object> builder = createBuilder(payload);
            for (Map.Entry<String, Object> entry : headers.entrySet()) {
                builder.property(entry.getKey(), entry.getValue());
            }
            builder.send();
        }

        // --- Receive ---

        @Override
        public Optional<Message<?>> receive() {
            return (receiveTimeout != null
                    ? dest.receiveTimeout(receiveTimeout).receiveMessage()
                    : dest.receiveMessage())
                    .map(ReceiveMessage::message)
                    .map(SpringJmsClientAdapter::toSpringMessage);
        }

        @Override
        public <T> Optional<T> receive(Class<T> targetClass) {
            return receiveTimeout != null
                    ? dest.receiveTimeout(receiveTimeout).receive(targetClass)
                    : dest.receive(targetClass);
        }

        @Override
        public Optional<Message<?>> receive(String messageSelector) {
            var builder = dest.select(messageSelector);
            if (receiveTimeout != null) {
                builder.receiveTimeout(receiveTimeout);
            }
            return builder.receiveMessage()
                    .map(ReceiveMessage::message)
                    .map(SpringJmsClientAdapter::toSpringMessage);
        }

        @Override
        public <T> Optional<T> receive(String messageSelector, Class<T> targetClass) {
            var builder = dest.select(messageSelector);
            if (receiveTimeout != null) {
                builder.receiveTimeout(receiveTimeout);
            }
            return builder.receive(targetClass);
        }

        // --- Send and Receive ---

        @Override
        public Optional<Message<?>> sendAndReceive(Message<?> requestMessage) {
            JmsSendBuilder<Object> builder = createBuilder(requestMessage.getPayload());
            for (Map.Entry<String, Object> entry : requestMessage.getHeaders().entrySet()) {
                if (!isInternalHeader(entry.getKey())) {
                    builder.property(entry.getKey(), entry.getValue());
                }
            }
            if (receiveTimeout != null) {
                builder.receiveTimeout(receiveTimeout);
            }
            try {
                Object reply = builder.sendAndReceive(Object.class);
                return Optional.of(new GenericMessage<>(reply));
            } catch (JmsReceiveException e) {
                return Optional.empty();
            }
        }

        @Override
        public <T> Optional<T> sendAndReceive(Object request, Class<T> targetClass) {
            JmsSendBuilder<Object> builder = createBuilder(request);
            if (receiveTimeout != null) {
                builder.receiveTimeout(receiveTimeout);
            }
            try {
                return Optional.ofNullable(builder.sendAndReceive(targetClass));
            } catch (JmsReceiveException e) {
                return Optional.empty();
            }
        }

        @Override
        public <T> Optional<T> sendAndReceive(Object request, Map<String, Object> headers,
                Class<T> targetClass) {
            JmsSendBuilder<Object> builder = createBuilder(request);
            for (Map.Entry<String, Object> entry : headers.entrySet()) {
                builder.property(entry.getKey(), entry.getValue());
            }
            if (receiveTimeout != null) {
                builder.receiveTimeout(receiveTimeout);
            }
            try {
                return Optional.ofNullable(builder.sendAndReceive(targetClass));
            } catch (JmsReceiveException e) {
                return Optional.empty();
            }
        }

        // --- Helpers ---

        private JmsSendBuilder<Object> createBuilder(Object payload) {
            JmsSendBuilder<Object> builder = dest.message(payload);
            if (deliveryDelay >= 0) {
                builder.deliveryDelay(Duration.ofMillis(deliveryDelay));
            }
            if (deliveryMode != null) {
                builder.deliveryMode(deliveryMode);
            }
            if (priority >= 0) {
                builder.priority(priority);
            }
            if (timeToLive >= 0) {
                builder.timeToLive(Duration.ofMillis(timeToLive));
            }
            return builder;
        }

        private static boolean isInternalHeader(String key) {
            return MessageHeaders.ID.equals(key)
                    || MessageHeaders.TIMESTAMP.equals(key);
        }
    }

    private static Message<?> toSpringMessage(jakarta.jms.Message jmsMessage) {
        try {
            Object body = jmsMessage.getBody(Object.class);
            if (body == null) {
                body = "";
            }
            Map<String, Object> headers = new HashMap<>();
            var names = jmsMessage.getPropertyNames();
            while (names.hasMoreElements()) {
                String name = (String) names.nextElement();
                headers.put(name, jmsMessage.getObjectProperty(name));
            }
            return new GenericMessage<>(body, headers);
        } catch (JMSException e) {
            throw new JmsException("Failed to convert JMS message to Spring Message", e);
        }
    }
}
