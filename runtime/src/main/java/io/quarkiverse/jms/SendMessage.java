package io.quarkiverse.jms;

import jakarta.jms.JMSException;
import jakarta.jms.Message;

/**
 * An outgoing JMS message wrapping a payload with an optional modifier
 * for advanced message configuration.
 *
 * <pre>{@code
 * // With modifier for low-level JMS message access
 * orders.message(SendMessage.of(order, msg -> msg.setStringProperty("status", "done")))
 *         .priority(5).send();
 *
 * // Without modifier — use message(payload) directly
 * orders.message(order).priority(5).send();
 * }</pre>
 *
 * @param <T> the payload type
 */
public record SendMessage<T>(T payload, JmsMessageModifier modifier) {

    /**
     * Create a {@code SendMessage} with the given payload and no modifier.
     */
    public static <T> SendMessage<T> of(T payload) {
        return new SendMessage<>(payload, null);
    }

    /**
     * Create a {@code SendMessage} with the given payload and a modifier for low-level JMS message access.
     */
    public static <T> SendMessage<T> of(T payload, JmsMessageModifier modifier) {
        return new SendMessage<>(payload, modifier);
    }

    /**
     * Apply the modifier to the converted JMS message.
     */
    public Message apply(Message message) {
        if (modifier != null) {
            try {
                modifier.modify(message);
            } catch (JMSException e) {
                throw new JmsException("Failed to apply message modifier", e);
            }
        }
        return message;
    }
}
