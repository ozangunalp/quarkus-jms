package io.quarkiverse.jms.deployment;

import java.util.List;

import io.quarkus.builder.item.MultiBuildItem;

/**
 * Build item representing a discovered {@code @JmsListener} method.
 */
public final class JmsListenerBuildItem extends MultiBuildItem {

    private final MethodInfo method;
    private final String destination;
    private final boolean topic;
    private final String selector;
    private final String connectionFactory;
    private final int concurrency;
    private final int maxConcurrency;
    private final String subscription;
    private final String acknowledgeMode;
    private final boolean runOnVirtualThread;
    private final String sendToDestination;
    private final boolean sendToTopic;

    public JmsListenerBuildItem(MethodInfo method, String destination,
            boolean topic, String selector, String connectionFactory, int concurrency,
            int maxConcurrency, String subscription, String acknowledgeMode,
            boolean runOnVirtualThread, String sendToDestination, boolean sendToTopic) {
        this.method = method;
        this.destination = destination;
        this.topic = topic;
        this.selector = selector;
        this.connectionFactory = connectionFactory;
        this.concurrency = concurrency;
        this.maxConcurrency = maxConcurrency;
        this.subscription = subscription;
        this.acknowledgeMode = acknowledgeMode;
        this.runOnVirtualThread = runOnVirtualThread;
        this.sendToDestination = sendToDestination;
        this.sendToTopic = sendToTopic;
    }

    public MethodInfo method() {
        return method;
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

    public String getSendToDestination() {
        return sendToDestination;
    }

    public boolean isSendToTopic() {
        return sendToTopic;
    }

    public boolean hasSendTo() {
        return sendToDestination != null;
    }

    public sealed interface Parameter {
        String type();

        record Payload(String type) implements Parameter {
        }

        record Header(String type, String headerName) implements Parameter {
            public boolean isAllHeaders() {
                return headerName == null;
            }
        }

        record RawMessage(String type) implements Parameter {
        }
    }

    public record MethodInfo(
            String beanClassName,
            String methodName,
            List<Parameter> parameters,
            String payloadType,
            String returnType) {

        public String invokerClassName() {
            return beanClassName.replace('$', '_') + "_" + methodName + "_JmsInvoker";
        }

        public List<String> allParameterTypes() {
            return parameters.stream().map(Parameter::type).toList();
        }
    }
}
