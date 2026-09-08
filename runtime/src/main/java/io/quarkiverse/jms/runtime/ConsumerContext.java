package io.quarkiverse.jms.runtime;

import jakarta.jms.JMSContext;
import jakarta.jms.Message;

/**
 * Callback interface used by {@link ConsumerWorker} to interact with the
 * listener registry without depending on its concrete implementation.
 */
interface ConsumerContext {

    void trackContext(JMSContext context);

    boolean isStopped();

    void spawnConsumer(ConsumerGroup group, boolean scaledUp);

    void onListenerError(JMSContext context, String destination, Throwable error, Message message);

    void reconnectWithBackoff(ConsumerGroup group);

    JmsInstrumentation instrumentation();
}
