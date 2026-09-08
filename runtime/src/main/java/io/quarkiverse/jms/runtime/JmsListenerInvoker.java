package io.quarkiverse.jms.runtime;

import jakarta.jms.JMSContext;
import jakarta.jms.Message;

/**
 * Interface for generated invoker classes. Each {@code @JmsListener} method
 * gets an invoker generated at build time via Gizmo that handles
 * CDI bean lookup, message conversion, and method dispatch.
 */
public interface JmsListenerInvoker {

    void invoke(Message message, JMSContext context);
}
