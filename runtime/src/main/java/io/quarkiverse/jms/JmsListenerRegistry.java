package io.quarkiverse.jms;

import java.io.Closeable;
import java.util.function.Consumer;

import jakarta.jms.Message;

/**
 * Programmatic registration of JMS listeners at runtime.
 * Inject and call {@link #register} to add listeners without annotations.
 *
 * <pre>{@code
 * @Inject
 * JmsListenerRegistry registry;
 *
 * void onStart(@Observes StartupEvent ev) {
 *     Closeable handle = registry.register("dynamic-queue", msg -> {
 *         String body = msg.getBody(String.class);
 *         // process
 *     });
 *     // later: handle.close() to stop the listener
 * }
 * }</pre>
 */
public interface JmsListenerRegistry {

    /**
     * Register a raw listener on a queue.
     *
     * @return a handle that stops the listener when closed
     */
    Closeable register(String destination, Consumer<Message> listener);

    /**
     * Register a raw listener with configuration.
     *
     * @return a handle that stops the listener when closed
     */
    Closeable register(ListenerConfig config, Consumer<Message> listener);

    /**
     * Register a typed listener on a queue. Messages are automatically
     * deserialized to the given type via {@link JmsMessageConverter}.
     *
     * @return a handle that stops the listener when closed
     */
    <T> Closeable register(String destination, Class<T> type, Consumer<T> listener);

    /**
     * Register a typed listener with configuration. Messages are automatically
     * deserialized to the given type via {@link JmsMessageConverter}.
     *
     * @return a handle that stops the listener when closed
     */
    <T> Closeable register(ListenerConfig config, Class<T> type, Consumer<T> listener);

    interface ListenerConfig {

        static ListenerConfig queue(String name) {
            return new DefaultListenerConfig(name, false, "", "", 1, -1, "", "AUTO", false);
        }

        static ListenerConfig topic(String name) {
            return new DefaultListenerConfig(name, true, "", "", 1, -1, "", "AUTO", false);
        }

        ListenerConfig selector(String selector);

        ListenerConfig connectionFactory(String identifier);

        ListenerConfig concurrency(int concurrency);

        ListenerConfig maxConcurrency(int maxConcurrency);

        ListenerConfig subscription(String subscription);

        ListenerConfig acknowledgeMode(String acknowledgeMode);

        ListenerConfig runOnVirtualThread();

        String destination();

        boolean isTopic();

        String selector();

        String connectionFactory();

        int concurrency();

        int maxConcurrency();

        String subscription();

        String acknowledgeMode();

        boolean isRunOnVirtualThread();
    }
}
