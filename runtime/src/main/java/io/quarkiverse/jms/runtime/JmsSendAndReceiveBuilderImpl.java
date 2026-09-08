package io.quarkiverse.jms.runtime;

import java.time.Duration;

import io.quarkiverse.jms.JmsSendAndReceiveBuilder;

class JmsSendAndReceiveBuilderImpl<T> extends JmsSendConfigImpl<T, JmsSendAndReceiveBuilder<T>>
        implements JmsSendAndReceiveBuilder<T> {

    JmsSendAndReceiveBuilderImpl(JmsSendConfigImpl<T, ?> source) {
        super(source);
    }

    @Override
    public JmsSendAndReceiveBuilder<T> select(String selector) {
        this.replySelector = selector;
        return this;
    }

    @Override
    public JmsSendAndReceiveBuilder<T> receiveTimeout(Duration timeout) {
        this.receiveTimeout = timeout;
        return this;
    }
}
