package io.quarkiverse.jms;

import jakarta.jms.JMSContext;
import jakarta.jms.Message;

/**
 * Callback for handling errors in {@link JmsListener @JmsListener} methods.
 * Implement as a CDI bean to customize error handling.
 *
 * <pre>{@code
 * @ApplicationScoped
 * public class MyErrorHandler implements JmsListenerErrorHandler {
 *     public void handleError(JMSContext context, Message message, String destination, Throwable error) {
 *         // custom alerting, metrics, dead-letter routing
 *     }
 * }
 * }</pre>
 */
public interface JmsListenerErrorHandler {

    /**
     * Called when a {@link JmsListener @JmsListener} method throws an exception.
     *
     * @param context the JMS context used by the listener (may be transacted)
     * @param message the JMS message that caused the error (may be null)
     * @param destination the destination name
     * @param error the exception thrown by the listener
     */
    void handleError(JMSContext context, Message message, String destination, Throwable error);
}
