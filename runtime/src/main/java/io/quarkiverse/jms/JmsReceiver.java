package io.quarkiverse.jms;

import java.time.Duration;
import java.util.Optional;

/**
 * A typed pull-based JMS receiver that holds a single consumer open
 * across multiple receive calls. Obtained via
 * {@link JmsDestination#receiveWith} — the framework manages the
 * consumer and session lifecycle.
 *
 * <pre>{@code
 * orders.receiveWith(receiver -> {
 *     receiver.receive().ifPresent(this::process);
 *     receiver.receive().ifPresent(this::process);
 * });
 * }</pre>
 *
 * @param <T> the message payload type
 */
public interface JmsReceiver<T> {

    Optional<T> receive();

    Optional<T> receive(Duration timeout);

    Optional<ReceiveMessage<T>> receiveMessage();

    Optional<ReceiveMessage<T>> receiveMessage(Duration timeout);
}
