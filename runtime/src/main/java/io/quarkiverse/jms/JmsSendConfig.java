package io.quarkiverse.jms;

import java.time.Duration;
import java.util.function.Function;

import jakarta.jms.Destination;
import jakarta.jms.JMSContext;
import jakarta.jms.Message;

/**
 * Common configuration for JMS send operations, shared by
 * {@link JmsSendBuilder} and {@link JmsSendAndReceiveBuilder}.
 *
 * @param <SELF> the concrete builder type (for fluent chaining)
 */
public interface JmsSendConfig<SELF extends JmsSendConfig<SELF>> {

    // --- Producer config ---

    /**
     * Set the JMS message priority (0-9, default is 4).
     */
    SELF priority(int priority);

    /**
     * Set the message time-to-live.
     */
    SELF timeToLive(Duration ttl);

    /**
     * Set the JMS delivery mode.
     */
    SELF deliveryMode(JmsDeliveryMode mode);

    /**
     * Set the minimum time before the message becomes visible to consumers.
     */
    SELF deliveryDelay(Duration delay);

    // --- Message config ---

    /**
     * Set a JMS message property.
     */
    SELF property(String key, Object value);

    /**
     * Set the JMS correlation ID on the outgoing message.
     */
    SELF correlationId(String id);

    /**
     * Set a correlation ID generator that derives the ID from the converted JMS {@link Message}.
     */
    SELF correlationId(Function<Message, String> generator);

    /**
     * Set the reply-to destination to the named queue.
     */
    SELF replyTo(String queue);

    /**
     * Set the reply-to destination using a factory that receives the {@link JMSContext}.
     */
    SELF replyTo(Function<JMSContext, Destination> factory);
}
