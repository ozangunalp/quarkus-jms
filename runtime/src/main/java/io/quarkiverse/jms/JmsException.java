package io.quarkiverse.jms;

/**
 * Base exception for Quarkus JMS operations.
 */
public class JmsException extends RuntimeException {

    public JmsException(String message) {
        super(message);
    }

    public JmsException(String message, Throwable cause) {
        super(message, cause);
    }
}
