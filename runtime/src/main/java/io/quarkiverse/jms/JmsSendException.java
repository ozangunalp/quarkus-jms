package io.quarkiverse.jms;

/**
 * Thrown when a JMS send operation fails.
 */
public class JmsSendException extends JmsException {

    public JmsSendException(String message, Throwable cause) {
        super(message, cause);
    }
}
