package io.quarkiverse.jms.runtime;

import java.util.Optional;

/**
 * Configuration for a named JMS destination, used with {@link io.quarkiverse.jms.JmsQueue @JmsQueue}
 * and {@link io.quarkiverse.jms.JmsTopic @JmsTopic} injection.
 */
public interface JmsDestinationConfig {

    /**
     * The JMS destination name (queue or topic name on the broker).
     * When not set, the configuration key is used as the destination name.
     */
    Optional<String> destination();

    /**
     * Named ConnectionFactory identifier.
     */
    Optional<String> connectionFactory();

    /**
     * JMS message selector for receive and browse operations.
     */
    Optional<String> selector();

    /**
     * Default message priority (0-9).
     */
    Optional<Integer> priority();

    /**
     * Time-to-live for outgoing messages (ISO-8601 duration, e.g., PT60S).
     */
    Optional<String> ttl();

    /**
     * Delivery delay for outgoing messages (ISO-8601 duration, e.g., PT5M).
     */
    Optional<String> deliveryDelay();

    /**
     * Delivery mode: {@code persistent} or {@code non_persistent}.
     */
    Optional<String> deliveryMode();
}
