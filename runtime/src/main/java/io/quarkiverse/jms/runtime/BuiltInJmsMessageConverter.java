package io.quarkiverse.jms.runtime;

import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;

import jakarta.inject.Singleton;
import jakarta.jms.BytesMessage;
import jakarta.jms.JMSContext;
import jakarta.jms.JMSException;
import jakarta.jms.MapMessage;
import jakarta.jms.Message;
import jakarta.jms.TextMessage;

import io.quarkiverse.jms.JmsConversionException;
import io.quarkiverse.jms.JmsMessageConverter;

/**
 * Handles built-in JMS types: {@link String}, {@code byte[]}, {@link Map},
 * and {@link Message} subtypes.
 * Higher priority than {@link JacksonJmsMessageConverter} — checked first in the converter chain.
 */
@Singleton
public class BuiltInJmsMessageConverter implements JmsMessageConverter {

    @Override
    public boolean supports(Class<?> type) {
        return type == String.class
                || type == byte[].class
                || type == Object.class
                || Map.class.isAssignableFrom(type)
                || Message.class.isAssignableFrom(type);
    }

    @Override
    public Message toMessage(Object payload, JMSContext context) {
        if (payload instanceof String s) {
            return context.createTextMessage(s);
        }
        if (payload instanceof byte[] bytes) {
            try {
                BytesMessage msg = context.createBytesMessage();
                msg.writeBytes(bytes);
                return msg;
            } catch (JMSException e) {
                throw new JmsConversionException("Failed to create BytesMessage", e);
            }
        }
        if (payload instanceof Map<?, ?> map) {
            try {
                MapMessage msg = context.createMapMessage();
                for (Map.Entry<?, ?> entry : map.entrySet()) {
                    msg.setObject(String.valueOf(entry.getKey()), entry.getValue());
                }
                return msg;
            } catch (JMSException e) {
                throw new JmsConversionException("Failed to create MapMessage", e);
            }
        }
        if (payload instanceof Message msg) {
            return msg;
        }
        throw new JmsConversionException("Unsupported built-in type: " + payload.getClass().getName(), null);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T fromMessage(Message message, Class<T> targetType) {
        try {
            if (Message.class.isAssignableFrom(targetType)) {
                return (T) message;
            }
            if (message instanceof TextMessage textMsg &&
                    (targetType == String.class || targetType == Object.class)) {
                return (T) textMsg.getText();
            }
            if (message instanceof BytesMessage bytesMsg &&
                    (targetType == byte[].class || targetType == Object.class)) {
                byte[] data = new byte[(int) bytesMsg.getBodyLength()];
                bytesMsg.readBytes(data);
                return (T) data;
            }
            if (message instanceof MapMessage mapMsg &&
                    (Map.class.isAssignableFrom(targetType) || targetType == Object.class)) {
                Map<String, Object> map = new HashMap<>();
                Enumeration<String> names = mapMsg.getMapNames();
                while (names.hasMoreElements()) {
                    String key = names.nextElement();
                    map.put(key, mapMsg.getObject(key));
                }
                return (T) map;
            }
            return message.getBody(targetType);
        } catch (JMSException e) {
            throw new JmsConversionException("Failed to read JMS message body", e);
        }
    }
}
