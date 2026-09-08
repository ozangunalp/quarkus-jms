package io.quarkiverse.jms.runtime;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import jakarta.jms.Destination;
import jakarta.jms.JMSContext;
import jakarta.jms.Message;

import io.quarkiverse.jms.JmsDeliveryMode;
import io.quarkiverse.jms.JmsSendConfig;
import io.quarkiverse.jms.ReceiveMessage;
import io.quarkiverse.jms.SendMessage;

abstract class JmsSendConfigImpl<T, SELF extends JmsSendConfig<SELF>> implements JmsSendConfig<SELF> {

    final JmsDestinationImpl<T> dest;
    final SendMessage<? extends T> sendMessage;

    Map<String, Object> properties;
    Function<Message, String> correlationIdProvider;
    Function<JMSContext, Destination> replyToFactory;
    long timeToLive;
    long deliveryDelay;
    int deliveryMode;
    int priority;
    String replySelector;
    Duration receiveTimeout;

    JmsSendConfigImpl(JmsDestinationImpl<T> dest, SendMessage<? extends T> sendMessage) {
        this.dest = dest;
        this.sendMessage = sendMessage;
        this.timeToLive = dest.defaultTimeToLive;
        this.deliveryDelay = dest.defaultDeliveryDelay;
        this.deliveryMode = dest.defaultDeliveryMode;
        this.priority = dest.defaultPriority;
        this.receiveTimeout = dest.client.defaultReceiveTimeout;
    }

    JmsSendConfigImpl(JmsSendConfigImpl<T, ?> source) {
        this.dest = source.dest;
        this.sendMessage = source.sendMessage;
        this.properties = source.properties;
        this.correlationIdProvider = source.correlationIdProvider;
        this.replyToFactory = source.replyToFactory;
        this.timeToLive = source.timeToLive;
        this.deliveryDelay = source.deliveryDelay;
        this.deliveryMode = source.deliveryMode;
        this.priority = source.priority;
        this.replySelector = source.replySelector;
        this.receiveTimeout = source.receiveTimeout;
    }

    @SuppressWarnings("unchecked")
    private SELF self() {
        return (SELF) this;
    }

    // --- Config methods (inherited by both builders) ---

    public SELF priority(int priority) {
        this.priority = priority;
        return self();
    }

    public SELF timeToLive(Duration ttl) {
        this.timeToLive = ttl.toMillis();
        return self();
    }

    public SELF deliveryMode(JmsDeliveryMode mode) {
        this.deliveryMode = mode.value();
        return self();
    }

    public SELF deliveryDelay(Duration delay) {
        this.deliveryDelay = delay.toMillis();
        return self();
    }

    public SELF property(String key, Object value) {
        if (properties == null) {
            properties = new HashMap<>();
        }
        properties.put(key, value);
        return self();
    }

    public SELF correlationId(String id) {
        this.correlationIdProvider = msg -> id;
        return self();
    }

    public SELF correlationId(Function<Message, String> generator) {
        this.correlationIdProvider = generator;
        return self();
    }

    public SELF replyTo(String queue) {
        this.replyToFactory = ctx -> ctx.createQueue(queue);
        return self();
    }

    public SELF replyTo(Function<JMSContext, Destination> factory) {
        this.replyToFactory = factory;
        return self();
    }

    // --- Terminal logic ---

    public void send() {
        dest.doSend(sendMessage, timeToLive, deliveryDelay, deliveryMode, priority,
                properties, correlationIdProvider, replyToFactory);
    }

    public <R> R sendAndReceive(Class<R> responseType) {
        Message reply = dest.doSendAndReceiveRaw(sendMessage, receiveTimeout, replySelector,
                timeToLive, deliveryDelay, deliveryMode, priority,
                properties, correlationIdProvider, replyToFactory);
        return dest.client.converterResolver.fromMessage(reply, responseType);
    }

    public <R> ReceiveMessage<R> sendAndReceiveMessage(Class<R> responseType) {
        Message reply = dest.doSendAndReceiveRaw(sendMessage, receiveTimeout, replySelector,
                timeToLive, deliveryDelay, deliveryMode, priority,
                properties, correlationIdProvider, replyToFactory);
        return dest.client.converterResolver.receivedFromMessage(reply, responseType);
    }
}
