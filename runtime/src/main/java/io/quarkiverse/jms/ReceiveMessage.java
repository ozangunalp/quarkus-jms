package io.quarkiverse.jms;

import jakarta.jms.Message;

/**
 * A received JMS message wrapping both the converted payload and the raw
 * {@link Message}.
 *
 * <pre>{@code
 * Optional<ReceiveMessage<Order>> msg = orders.receiveMessage();
 * msg.ifPresent(m -> {
 *     Order order = m.payload();
 *     String corrId = m.message().getJMSCorrelationID();
 * });
 * }</pre>
 *
 * @param payload the deserialized payload
 * @param message the raw JMS message
 */
public record ReceiveMessage<T>(T payload, Message message) {
}
