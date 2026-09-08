package io.quarkiverse.jms;

import java.util.List;

/**
 * A strongly-typed JMS queue destination.
 *
 * <pre>{@code
 * // Browse all messages
 * List<Order> all = queue.browse();
 *
 * // Browse with selector
 * List<Order> vip = queue.browse("priority > 3");
 *
 * // Browse with raw JMS message access
 * queue.browseMessages().stream()
 *         .filter(m -> m.payload().isUrgent())
 *         .forEach(m -> log.info(m.message().getJMSMessageID()));
 * }</pre>
 *
 * @param <T> the message payload type
 */
public interface JmsQueue<T> extends JmsDestination<T> {

    /**
     * Browse all messages on this queue without consuming them.
     */
    List<T> browse();

    /**
     * Browse messages matching the selector without consuming them.
     */
    List<T> browse(String selector);

    /**
     * Browse all messages on this queue as {@link ReceiveMessage}, including the raw JMS message.
     */
    List<ReceiveMessage<T>> browseMessages();

    /**
     * Browse messages matching the selector as {@link ReceiveMessage}.
     */
    List<ReceiveMessage<T>> browseMessages(String selector);
}
