package io.quarkiverse.jms.runtime;

import java.time.Duration;
import java.util.Optional;

import jakarta.jms.JMSConsumer;
import jakarta.jms.Message;

import io.quarkiverse.jms.JmsReceiver;
import io.quarkiverse.jms.ReceiveMessage;

class JmsReceiverImpl<T> implements JmsReceiver<T>, AutoCloseable {

    private final JMSConsumer consumer;
    private final JmsMessageConverterResolver converterResolver;
    private final JmsInstrumentation instrumentation;
    private final String destinationName;
    private final boolean isTopic;
    private final Class<T> messageType;
    private final Duration defaultTimeout;

    JmsReceiverImpl(JMSConsumer consumer, JmsMessageConverterResolver converterResolver,
            JmsInstrumentation instrumentation, String destinationName, boolean isTopic,
            Class<T> messageType, Duration defaultTimeout) {
        this.consumer = consumer;
        this.converterResolver = converterResolver;
        this.instrumentation = instrumentation;
        this.destinationName = destinationName;
        this.isTopic = isTopic;
        this.messageType = messageType;
        this.defaultTimeout = defaultTimeout;
    }

    @Override
    public Optional<T> receive() {
        return receive(defaultTimeout);
    }

    @Override
    public Optional<T> receive(Duration timeout) {
        Message message = doReceive(timeout);
        if (message == null) {
            return Optional.empty();
        }
        return Optional.of(converterResolver.fromMessage(message, messageType));
    }

    @Override
    public Optional<ReceiveMessage<T>> receiveMessage() {
        return receiveMessage(defaultTimeout);
    }

    @Override
    public Optional<ReceiveMessage<T>> receiveMessage(Duration timeout) {
        Message message = doReceive(timeout);
        if (message == null) {
            return Optional.empty();
        }
        return Optional.of(converterResolver.receivedFromMessage(message, messageType));
    }

    @Override
    public void close() {
        try {
            consumer.close();
        } catch (Exception e) {
            // ignore
        }
    }

    private Message doReceive(Duration timeout) {
        JmsInstrumentation.Scope scope = instrumentation.onReceive(destinationName, isTopic);
        try {
            Message message = consumer.receive(timeout.toMillis());
            scope.onMessage(message);
            return message;
        } catch (Throwable t) {
            scope.onError(t);
            throw t;
        } finally {
            scope.close();
        }
    }
}
