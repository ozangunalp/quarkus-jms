package io.quarkiverse.jms.runtime;

import java.time.Duration;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;

import io.quarkiverse.jms.JmsReceiveBuilder;
import io.quarkiverse.jms.JmsReceiver;
import io.quarkiverse.jms.ReceiveMessage;

class JmsReceiveBuilderImpl<T> implements JmsReceiveBuilder<T> {

    private final JmsDestinationImpl<T> dest;
    private String selector;
    private Duration receiveTimeout;

    JmsReceiveBuilderImpl(JmsDestinationImpl<T> dest) {
        this.dest = dest;
        this.selector = dest.defaultSelector;
        this.receiveTimeout = dest.client.defaultReceiveTimeout;
    }

    @Override
    public JmsReceiveBuilder<T> select(String selector) {
        this.selector = selector;
        return this;
    }

    @Override
    public JmsReceiveBuilder<T> receiveTimeout(Duration timeout) {
        this.receiveTimeout = timeout;
        return this;
    }

    @Override
    public Optional<T> receive() {
        return dest.doReceive(dest.messageType, selector, receiveTimeout);
    }

    @Override
    public Optional<ReceiveMessage<T>> receiveMessage() {
        return dest.doReceiveMessage(dest.messageType, selector, receiveTimeout);
    }

    @Override
    public <U> Optional<U> receive(Class<U> type) {
        return dest.doReceive(type, selector, receiveTimeout);
    }

    @Override
    public <U> Optional<ReceiveMessage<U>> receiveMessage(Class<U> type) {
        return dest.doReceiveMessage(type, selector, receiveTimeout);
    }

    @Override
    public void receiveWith(Consumer<JmsReceiver<T>> callback) {
        receiveWith(receiver -> {
            callback.accept(receiver);
            return null;
        });
    }

    @Override
    public <R> R receiveWith(Function<JmsReceiver<T>, R> callback) {
        return dest.doReceiveWith(selector, receiveTimeout, callback);
    }

}
