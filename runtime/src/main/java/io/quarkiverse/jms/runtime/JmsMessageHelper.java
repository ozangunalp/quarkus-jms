package io.quarkiverse.jms.runtime;

import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;

import jakarta.jms.JMSException;
import jakarta.jms.Message;

import io.quarkiverse.jms.JmsException;

/**
 * Utility methods used by generated invokers.
 */
public class JmsMessageHelper {

    private JmsMessageHelper() {
    }

    public static Map<String, Object> extractProperties(Message message) {
        Map<String, Object> properties = new HashMap<>();
        try {
            Enumeration<?> names = message.getPropertyNames();
            while (names.hasMoreElements()) {
                String name = (String) names.nextElement();
                properties.put(name, message.getObjectProperty(name));
            }
        } catch (JMSException e) {
            throw new JmsException("Failed to extract JMS message properties", e);
        }
        return properties;
    }
}
