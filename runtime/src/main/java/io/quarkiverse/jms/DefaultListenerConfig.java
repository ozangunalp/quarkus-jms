package io.quarkiverse.jms;

record DefaultListenerConfig(String destination, boolean isTopic, String selector,
        String connectionFactory, int concurrency, int maxConcurrency,
        String subscription, String acknowledgeMode,
        boolean isRunOnVirtualThread) implements JmsListenerRegistry.ListenerConfig {

    @Override
    public JmsListenerRegistry.ListenerConfig selector(String selector) {
        return new DefaultListenerConfig(destination, isTopic, selector, connectionFactory, concurrency, maxConcurrency,
                subscription, acknowledgeMode, isRunOnVirtualThread);
    }

    @Override
    public JmsListenerRegistry.ListenerConfig connectionFactory(String identifier) {
        return new DefaultListenerConfig(destination, isTopic, selector, identifier, concurrency, maxConcurrency,
                subscription, acknowledgeMode, isRunOnVirtualThread);
    }

    @Override
    public JmsListenerRegistry.ListenerConfig concurrency(int concurrency) {
        return new DefaultListenerConfig(destination, isTopic, selector, connectionFactory, concurrency, maxConcurrency,
                subscription, acknowledgeMode, isRunOnVirtualThread);
    }

    @Override
    public JmsListenerRegistry.ListenerConfig maxConcurrency(int maxConcurrency) {
        return new DefaultListenerConfig(destination, isTopic, selector, connectionFactory, concurrency, maxConcurrency,
                subscription, acknowledgeMode, isRunOnVirtualThread);
    }

    @Override
    public JmsListenerRegistry.ListenerConfig subscription(String subscription) {
        return new DefaultListenerConfig(destination, isTopic, selector, connectionFactory, concurrency, maxConcurrency,
                subscription, acknowledgeMode, isRunOnVirtualThread);
    }

    @Override
    public JmsListenerRegistry.ListenerConfig acknowledgeMode(String acknowledgeMode) {
        return new DefaultListenerConfig(destination, isTopic, selector, connectionFactory, concurrency, maxConcurrency,
                subscription, acknowledgeMode, isRunOnVirtualThread);
    }

    @Override
    public JmsListenerRegistry.ListenerConfig runOnVirtualThread() {
        return new DefaultListenerConfig(destination, isTopic, selector, connectionFactory, concurrency, maxConcurrency,
                subscription, acknowledgeMode, true);
    }
}
