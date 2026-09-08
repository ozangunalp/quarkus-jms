package io.quarkiverse.jms;

import java.time.Duration;

/**
 * Builder for send-and-receive operations, entered from
 * {@link JmsSendBuilder#select(String)} or {@link JmsSendBuilder#receiveTimeout(Duration)}.
 * Only exposes {@code sendAndReceive} terminals (not {@code send}).
 *
 * @param <T> the message payload type
 */
public interface JmsSendAndReceiveBuilder<T> extends JmsSendConfig<JmsSendAndReceiveBuilder<T>> {

    /**
     * Set a JMS message selector for the reply.
     */
    JmsSendAndReceiveBuilder<T> select(String selector);

    /**
     * Set a receive timeout for the reply.
     */
    JmsSendAndReceiveBuilder<T> receiveTimeout(Duration timeout);

    /**
     * Send the message and synchronously wait for a reply, deserialized to the given type.
     *
     * @throws JmsReceiveException if no reply is received before the timeout expires
     */
    <R> R sendAndReceive(Class<R> responseType);

    /**
     * Send the message and synchronously wait for a reply as a {@link ReceiveMessage}.
     *
     * @throws JmsReceiveException if no reply is received before the timeout expires
     */
    <R> ReceiveMessage<R> sendAndReceiveMessage(Class<R> responseType);
}
