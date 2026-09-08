package io.quarkiverse.jms;

/**
 * A strongly-typed JMS topic destination.
 *
 * @param <T> the message payload type
 */
public interface JmsTopic<T> extends JmsDestination<T> {
}
