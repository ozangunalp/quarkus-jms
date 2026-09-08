package io.quarkiverse.jms;

/**
 * Thrown when a JMS receive operation fails.
 */
public class JmsReceiveException extends JmsException {

    public JmsReceiveException(String message, Throwable cause) {
        super(message, cause);
    }
}
