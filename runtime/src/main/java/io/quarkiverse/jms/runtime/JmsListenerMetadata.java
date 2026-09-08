package io.quarkiverse.jms.runtime;

import io.quarkus.runtime.annotations.RecordableConstructor;

/**
 * Metadata for a discovered {@code @JmsListener} method, recorded at build time
 * and used by {@link JmsListenerRegistryImpl} at runtime to create consumers.
 */
public class JmsListenerMetadata {

    private final String beanClassName;
    private final String methodName;
    private final String destination;
    private final boolean topic;
    private final String selector;
    private final String connectionFactory;
    private final int concurrency;
    private final int maxConcurrency;
    private final String subscription;
    private final String acknowledgeMode;
    private final boolean runOnVirtualThread;
    private final String invokerClassName;
    private final String parameterType;

    @RecordableConstructor
    public JmsListenerMetadata(String beanClassName, String methodName, String destination,
            boolean topic, String selector, String connectionFactory, int concurrency,
            int maxConcurrency, String subscription, String acknowledgeMode, boolean runOnVirtualThread,
            String invokerClassName, String parameterType) {
        this.beanClassName = beanClassName;
        this.methodName = methodName;
        this.destination = destination;
        this.topic = topic;
        this.selector = selector;
        this.connectionFactory = connectionFactory;
        this.concurrency = concurrency;
        this.maxConcurrency = maxConcurrency;
        this.subscription = subscription;
        this.acknowledgeMode = acknowledgeMode;
        this.runOnVirtualThread = runOnVirtualThread;
        this.invokerClassName = invokerClassName;
        this.parameterType = parameterType;
    }

    public String getBeanClassName() {
        return beanClassName;
    }

    public String getMethodName() {
        return methodName;
    }

    public String getDestination() {
        return destination;
    }

    public boolean isTopic() {
        return topic;
    }

    public String getSelector() {
        return selector;
    }

    public String getConnectionFactory() {
        return connectionFactory;
    }

    public int getConcurrency() {
        return concurrency;
    }

    public int getMaxConcurrency() {
        return maxConcurrency;
    }

    public String getSubscription() {
        return subscription;
    }

    public String getAcknowledgeMode() {
        return acknowledgeMode;
    }

    public boolean isRunOnVirtualThread() {
        return runOnVirtualThread;
    }

    public String getInvokerClassName() {
        return invokerClassName;
    }

    public String getParameterType() {
        return parameterType;
    }
}
