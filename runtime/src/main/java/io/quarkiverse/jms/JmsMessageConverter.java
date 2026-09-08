package io.quarkiverse.jms;

import jakarta.jms.JMSContext;
import jakarta.jms.Message;

/**
 * SPI for converting between JMS messages and application objects.
 * Implementations are CDI beans — provide an {@link jakarta.enterprise.inject.Alternative}
 * with higher priority to override the default converter.
 */
public interface JmsMessageConverter {

    /**
     * Convert a payload object to a JMS message.
     */
    Message toMessage(Object payload, JMSContext context);

    /**
     * Convert a JMS message to the given target type.
     */
    <T> T fromMessage(Message message, Class<T> targetType);

    /**
     * Whether this converter supports the given type.
     */
    boolean supports(Class<?> type);
}
