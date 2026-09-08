package io.quarkiverse.jms.runtime;

import java.util.HashMap;
import java.util.Map;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import jakarta.jms.JMSContext;
import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.TextMessage;

import org.jboss.logging.Logger;

import io.quarkiverse.jms.JmsConversionException;
import io.quarkiverse.jms.JmsMessageConverter;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;

/**
 * Converts POJOs to/from JSON via Jackson {@link ObjectMapper}.
 * Catch-all converter — handles any type not covered by
 * {@link BuiltInJmsMessageConverter} or custom converters.
 */
@Singleton
public class JacksonJmsMessageConverter implements JmsMessageConverter {

    private static final Logger LOG = Logger.getLogger(JacksonJmsMessageConverter.class);

    private final ObjectMapper objectMapper;
    private final String typeIdProperty;
    private final Map<String, String> nameToClass;
    private final Map<String, String> classToName;

    @Inject
    public JacksonJmsMessageConverter(ObjectMapper objectMapper, JmsRuntimeConfig config) {
        this.objectMapper = objectMapper;
        this.typeIdProperty = config.converter().typeIdProperty();
        this.nameToClass = new HashMap<>(config.converter().typeIdMappings());
        this.classToName = new HashMap<>();
        this.nameToClass.forEach((name, fqcn) -> classToName.put(fqcn, name));
    }

    @Override
    public Message toMessage(Object payload, JMSContext context) {
        if (payload == null) {
            throw new IllegalArgumentException("Payload must not be null");
        }
        try {
            String json = objectMapper.writeValueAsString(payload);
            TextMessage msg = context.createTextMessage(json);
            String typeId = classToName.getOrDefault(payload.getClass().getName(),
                    payload.getClass().getName());
            msg.setStringProperty(typeIdProperty, typeId);
            return msg;
        } catch (JacksonException e) {
            throw new JmsConversionException("Failed to serialize payload to JSON", e);
        } catch (JMSException e) {
            throw new JmsConversionException("Failed to create JMS message", e);
        }
    }

    @Override
    public <T> T fromMessage(Message message, Class<T> targetType) {
        try {
            if (!(message instanceof TextMessage textMsg)) {
                return message.getBody(targetType);
            }
            String text = textMsg.getText();
            if (targetType == Object.class) {
                Class<?> resolved = resolveTypeFromMessage(message);
                if (resolved != null) {
                    @SuppressWarnings("unchecked")
                    T result = (T) objectMapper.readValue(text, resolved);
                    return result;
                }
            }
            return objectMapper.readValue(text, targetType);
        } catch (JMSException e) {
            throw new JmsConversionException("Failed to read JMS message body", e);
        } catch (JacksonException e) {
            throw new JmsConversionException("Failed to deserialize JSON from JMS message", e);
        }
    }

    private Class<?> resolveTypeFromMessage(Message message) {
        try {
            String typeId = message.getStringProperty(typeIdProperty);
            if (typeId == null || typeId.isEmpty()) {
                return null;
            }
            String fqcn = nameToClass.get(typeId);
            if (fqcn == null) {
                LOG.warnf("Type ID '%s' is not in the configured type-id-mappings, falling back to raw deserialization",
                        typeId);
                return null;
            }
            return Thread.currentThread().getContextClassLoader().loadClass(fqcn);
        } catch (ClassNotFoundException e) {
            LOG.warnf("Type ID '%s' could not be resolved to a class, falling back to raw deserialization",
                    e.getMessage());
            return null;
        } catch (JMSException e) {
            return null;
        }
    }

    @Override
    public boolean supports(Class<?> type) {
        return true;
    }
}
