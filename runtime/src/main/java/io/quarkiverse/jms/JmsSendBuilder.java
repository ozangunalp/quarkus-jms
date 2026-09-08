package io.quarkiverse.jms;

import java.time.Duration;

/**
 * Per-call builder for sending a JMS message, started from {@link JmsDestination#message(Object)}.
 *
 * <pre>{@code
 * // Send with config
 * queue.message(order).priority(5).correlationId("req-123").send();
 *
 * // Request/reply (narrows via select/receiveTimeout)
 * queue.message(order).receiveTimeout(Duration.ofSeconds(30)).sendAndReceive(Reply.class);
 *
 * // With modifier
 * queue.message(SendMessage.of(order, msg -> msg.setStringProperty("k", "v")))
 *         .priority(5).send();
 * }</pre>
 *
 * @param <T> the message payload type
 */
public interface JmsSendBuilder<T> extends JmsSendConfig<JmsSendBuilder<T>> {

    // --- Narrows to sendAndReceive-only ---

    /**
     * Set a JMS message selector for the reply and narrow to a {@link JmsSendAndReceiveBuilder}.
     */
    JmsSendAndReceiveBuilder<T> select(String selector);

    /**
     * Set a receive timeout for the reply and narrow to a {@link JmsSendAndReceiveBuilder}.
     */
    JmsSendAndReceiveBuilder<T> receiveTimeout(Duration timeout);

    // --- Terminals ---

    /**
     * Send the message to the destination.
     */
    void send();

    /**
     * Send the message and synchronously wait for a reply, deserialized to the given type.
     * Creates a temporary queue for the reply unless {@link #replyTo(String)} is set.
     *
     * @throws JmsReceiveException if no reply is received before the timeout expires
     */
    <R> R sendAndReceive(Class<R> responseType);

    /**
     * Send the message and synchronously wait for a reply as a {@link ReceiveMessage}.
     * Creates a temporary queue for the reply unless {@link #replyTo(String)} is set.
     *
     * @throws JmsReceiveException if no reply is received before the timeout expires
     */
    <R> ReceiveMessage<R> sendAndReceiveMessage(Class<R> responseType);
}
