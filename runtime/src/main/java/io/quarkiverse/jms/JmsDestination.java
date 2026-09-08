package io.quarkiverse.jms;

import java.time.Duration;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;

import jakarta.jms.JMSContext;
import jakarta.jms.Message;

/**
 * An immutable, strongly-typed JMS destination.
 *
 * <pre>{@code
 * // Simple operations
 * orders.send(order);
 * orders.receive();
 *
 * // Send with config
 * orders.message(order).priority(5).correlationId("req-123").send();
 *
 * // Receive with selector and timeout
 * orders.select("urgency = 'HIGH'").receiveTimeout(Duration.ofSeconds(30)).receive();
 *
 * // Request/reply
 * orders.message(order).sendAndReceive(Reply.class);
 *
 * // With modifier
 * orders.message(SendMessage.of(order, msg -> msg.setStringProperty("k", "v"))).send();
 * }</pre>
 *
 * @param <T> the message payload type
 */
public interface JmsDestination<T> {

    // --- Message builder ---

    /**
     * Start building a send operation for the given payload.
     */
    JmsSendBuilder<T> message(T payload);

    /**
     * Start building a send operation for the given {@link SendMessage}, which may include a message modifier.
     */
    JmsSendBuilder<T> message(SendMessage<? extends T> sendMessage);

    // --- Receive builder ---

    /**
     * Start building a receive operation with the given JMS message selector.
     */
    JmsReceiveBuilder<T> select(String selector);

    /**
     * Start building a receive operation with the given timeout.
     */
    JmsReceiveBuilder<T> receiveTimeout(Duration timeout);

    // --- Convenience send ---

    /**
     * Send a payload to this destination using default producer settings.
     */
    void send(T payload);

    /**
     * Send a {@link SendMessage} to this destination using default producer settings.
     */
    void send(SendMessage<? extends T> sendMessage);

    /**
     * Send a raw JMS {@link Message} created by the given callback.
     */
    void send(Function<JMSContext, Message> messageCreator);

    // --- Convenience receive ---

    /**
     * Receive and deserialize the next message, or empty if none is available before the timeout.
     */
    Optional<T> receive();

    /**
     * Receive the next message as a {@link ReceiveMessage} including the raw JMS message, or empty if none is available.
     */
    Optional<ReceiveMessage<T>> receiveMessage();

    /**
     * Receive and deserialize the next message as the given type, or empty if none is available.
     */
    <U> Optional<U> receive(Class<U> type);

    /**
     * Receive the next message as a {@link ReceiveMessage} deserialized to the given type, or empty if none is available.
     */
    <U> Optional<ReceiveMessage<U>> receiveMessage(Class<U> type);

    // --- Convenience request/reply ---

    /**
     * Send a payload and synchronously wait for a reply, deserialized to the given type.
     * Creates a temporary queue for the reply unless a reply-to destination is set via the
     * {@linkplain #message(Object) send builder}. When a shared reply queue is set via
     * {@code replyTo()} without an explicit correlation ID, one is auto-generated and
     * reply filtering is handled automatically.
     *
     * @throws JmsReceiveException if no reply is received before the timeout expires
     */
    <R> R sendAndReceive(T payload, Class<R> responseType);

    /**
     * Send a {@link SendMessage} and synchronously wait for a reply, deserialized to the given type.
     *
     * @throws JmsReceiveException if no reply is received before the timeout expires
     */
    <R> R sendAndReceive(SendMessage<? extends T> sendMessage, Class<R> responseType);

    /**
     * Send a payload and synchronously wait for a reply as a {@link ReceiveMessage}.
     *
     * @throws JmsReceiveException if no reply is received before the timeout expires
     */
    <R> ReceiveMessage<R> sendAndReceiveMessage(T payload, Class<R> responseType);

    /**
     * Send a {@link SendMessage} and synchronously wait for a reply as a {@link ReceiveMessage}.
     */
    <R> ReceiveMessage<R> sendAndReceiveMessage(SendMessage<? extends T> sendMessage, Class<R> responseType);

    // --- Multi-receive ---

    /**
     * Open a long-lived consumer and pass it to the callback for multiple typed receives.
     * The consumer and session are closed automatically when the callback returns.
     */
    void receiveWith(Consumer<JmsReceiver<T>> callback);

    /**
     * Open a long-lived consumer and pass it to the callback for multiple typed receives.
     * The consumer and session are closed automatically when the callback returns.
     */
    <R> R receiveWith(Function<JmsReceiver<T>, R> callback);

    // --- Low-level ---

    /**
     * Execute an arbitrary operation on the underlying {@link JMSContext} for this destination.
     */
    <U> U execute(Function<JMSContext, U> callback);
}
