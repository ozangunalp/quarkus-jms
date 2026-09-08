package io.quarkiverse.jms;

/**
 * Thrown when a JMS listener processing fails.
 */
public class JmsListenerException extends JmsException {

    public JmsListenerException(String message, Throwable cause) {
        super(message, cause);
    }
}
