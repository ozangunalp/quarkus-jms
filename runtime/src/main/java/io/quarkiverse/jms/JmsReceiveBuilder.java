package io.quarkiverse.jms;

import java.time.Duration;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Per-call builder for receiving JMS messages, started from
 * {@link JmsDestination#select(String)} or {@link JmsDestination#receiveTimeout(Duration)}.
 *
 * <pre>{@code
 * queue.select("urgency = 'HIGH'").receive();
 * queue.receiveTimeout(Duration.ofSeconds(30)).receive(String.class);
 * }</pre>
 *
 * @param <T> the message payload type
 */
public interface JmsReceiveBuilder<T> {

    /**
     * Set a JMS message selector to filter received messages.
     */
    JmsReceiveBuilder<T> select(String selector);

    /**
     * Set a receive timeout for the receive operation.
     */
    JmsReceiveBuilder<T> receiveTimeout(Duration timeout);

    // --- Receive builder ---

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

    /**
     * Open a long-lived consumer with the configured selector and timeout,
     * and pass it to the callback for multiple typed receives.
     */
    void receiveWith(Consumer<JmsReceiver<T>> callback);

    /**
     * Open a long-lived consumer with the configured selector and timeout,
     * and pass it to the callback for multiple typed receives.
     */
    <R> R receiveWith(Function<JmsReceiver<T>, R> callback);

}
