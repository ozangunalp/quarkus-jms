package io.quarkiverse.jms;

import jakarta.jms.JMSException;
import jakarta.jms.Message;

/**
 * A modifier applied to a converted JMS {@link Message} before sending.
 * Unlike {@link java.util.function.Consumer}, this interface declares
 * {@code throws JMSException} so JMS property methods can be called directly.
 *
 * <pre>{@code
 * SendMessage.of(order, msg -> msg.setStringProperty("status", "done"))
 * }</pre>
 */
@FunctionalInterface
public interface JmsMessageModifier {

    void modify(Message message) throws JMSException;
}
