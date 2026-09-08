package io.quarkiverse.jms.runtime;

import java.time.Duration;

import io.quarkiverse.jms.JmsSendAndReceiveBuilder;
import io.quarkiverse.jms.JmsSendBuilder;
import io.quarkiverse.jms.SendMessage;

class JmsSendBuilderImpl<T> extends JmsSendConfigImpl<T, JmsSendBuilder<T>> implements JmsSendBuilder<T> {

    JmsSendBuilderImpl(JmsDestinationImpl<T> dest, SendMessage<? extends T> sendMessage) {
        super(dest, sendMessage);
    }

    @Override
    public JmsSendAndReceiveBuilder<T> select(String selector) {
        return new JmsSendAndReceiveBuilderImpl<>(this)
                .select(selector);
    }

    @Override
    public JmsSendAndReceiveBuilder<T> receiveTimeout(Duration timeout) {
        return new JmsSendAndReceiveBuilderImpl<>(this)
                .receiveTimeout(timeout);
    }
}
