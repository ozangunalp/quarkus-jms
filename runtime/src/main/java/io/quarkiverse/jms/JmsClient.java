package io.quarkiverse.jms;

/**
 * Fluent API for sending and receiving JMS messages. Injected as a CDI bean.
 *
 * <pre>
 * {
 *     &#64;code
 *     &#64;Inject
 *     JmsClient jms;
 *
 *     // Typed — send/receive are type-safe
 *     JmsQueue<Order> orders = jms.queue("orders", Order.class);
 *     orders.send(order);
 *     Optional<Order> order = orders.receive();
 *
 *     // Untyped — use explicit class on receive
 *     jms.queue("orders").send(order);
 *     Optional<Order> order = jms.queue("orders").receiveTimeout(Duration.ofSeconds(5)).receive(Order.class);
 *
 *     // Injected typed destinations
 *     &#64;Inject
 *     &#64;Identifier("orders")
 *     JmsQueue<Order> orders;
 *     &#64;Inject
 *     &#64;Identifier("events")
 *     JmsTopic<Event> events;
 * }
 * </pre>
 */
public interface JmsClient {

    /**
     * Create a typed queue destination.
     */
    <T> JmsQueue<T> queue(String name, Class<T> type);

    /**
     * Create an untyped queue destination. Use explicit-class receive methods
     * ({@code receive(Class)}) for deserialization.
     */
    JmsQueue<Object> queue(String name);

    /**
     * Create a typed topic destination.
     */
    <T> JmsTopic<T> topic(String name, Class<T> type);

    /**
     * Create an untyped topic destination. Use explicit-class receive methods
     * ({@code receive(Class)}) for deserialization.
     */
    JmsTopic<Object> topic(String name);

    /**
     * Force reset of the cached JMS connection. All cached sessions, consumers,
     * and temporary destinations are closed. A new connection will be created
     * on the next operation.
     * <p>
     * Thread-safe: can be called from any thread. In-flight operations on existing
     * {@link JmsDestination} references will complete on their current session before
     * the reset takes effect. Subsequent operations will use a new connection.
     */
    void resetConnection();
}
