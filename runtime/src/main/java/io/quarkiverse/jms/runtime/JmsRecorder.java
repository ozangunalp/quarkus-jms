package io.quarkiverse.jms.runtime;

import java.util.List;
import java.util.function.Supplier;

import org.jboss.logging.Logger;

import io.quarkus.arc.Arc;
import io.quarkus.runtime.annotations.Recorder;

@Recorder
public class JmsRecorder {

    private static final Logger LOG = Logger.getLogger(JmsRecorder.class);

    public void initializeListenerContainer(List<JmsListenerMetadata> listeners) {
        JmsListenerRegistryImpl container = Arc.container().instance(JmsListenerRegistryImpl.class).get();
        container.startListeners(listeners);
    }

    public void disableBuiltInPooling() {
        JmsClientImpl client = Arc.container().instance(JmsClientImpl.class).get();
        client.setExternalPoolingPresent(true);
    }

    @SuppressWarnings({ "rawtypes" })
    public Supplier destinationSupplier(String name, boolean isTopic, String messageTypeName) {
        return () -> {
            Class<?> messageType;
            try {
                messageType = Class.forName(messageTypeName, true, Thread.currentThread().getContextClassLoader());
            } catch (ClassNotFoundException e) {
                LOG.warnf("Message type class '%s' not found for destination '%s', falling back to Object",
                        messageTypeName, name);
                messageType = Object.class;
            }
            JmsClientImpl client = Arc.container().instance(JmsClientImpl.class).get();
            JmsRuntimeConfig config = Arc.container().instance(JmsRuntimeConfig.class).get();
            JmsDestinationConfig destConfig = config.destinations().get(name);
            if (destConfig != null) {
                String destName = destConfig.destination().orElse(name);
                return client.createConfigured(destName, isTopic, messageType, destConfig);
            }
            return isTopic ? client.topic(name, messageType) : client.queue(name, messageType);
        };
    }
}
