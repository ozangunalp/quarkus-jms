package io.quarkiverse.jms;

/**
 * Thrown when message conversion (serialization or deserialization) fails.
 */
public class JmsConversionException extends JmsException {

    public JmsConversionException(String message, Throwable cause) {
        super(message, cause);
    }
}
