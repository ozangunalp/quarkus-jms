package io.quarkiverse.jms;

/**
 * Return type for {@link JmsListener @JmsListener} methods that specifies
 * the reply destination dynamically. Alternative to {@link SendTo @SendTo}
 * when the destination is determined at runtime.
 *
 * <pre>{@code
 * @JmsListener("requests")
 * public JmsResponse<Order> handle(Request request) {
 *     Order order = process(request);
 *     return JmsResponse.forQueue(order, "dynamic-reply-queue");
 * }
 * }</pre>
 */
public class JmsResponse<T> {

    private final T payload;
    private final String destination;
    private final boolean topic;

    private JmsResponse(T payload, String destination, boolean topic) {
        this.payload = payload;
        this.destination = destination;
        this.topic = topic;
    }

    /**
     * Create a response targeting a queue.
     */
    public static <T> JmsResponse<T> forQueue(T payload, String queue) {
        return new JmsResponse<>(payload, queue, false);
    }

    /**
     * Create a response targeting a topic.
     */
    public static <T> JmsResponse<T> forTopic(T payload, String topic) {
        return new JmsResponse<>(payload, topic, true);
    }

    public T getResponse() {
        return payload;
    }

    public String getDestination() {
        return destination;
    }

    public boolean isTopic() {
        return topic;
    }
}
