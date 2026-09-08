package io.quarkiverse.jms.deployment;

import jakarta.enterprise.context.ApplicationScoped;

import org.jboss.jandex.DotName;

import io.quarkiverse.jms.JmsHeader;
import io.quarkiverse.jms.JmsListener;
import io.quarkiverse.jms.JmsListeners;
import io.quarkiverse.jms.JmsMessageConverter;
import io.quarkiverse.jms.JmsQueue;
import io.quarkiverse.jms.JmsTopic;
import io.quarkiverse.jms.SendTo;
import io.quarkiverse.jms.runtime.JacksonJmsMessageConverter;
import io.smallrye.common.annotation.Identifier;

final class DotNames {

    static final DotName JMS_LISTENER = DotName.createSimple(JmsListener.class);
    static final DotName JMS_LISTENERS = DotName.createSimple(JmsListeners.class);
    static final DotName JMS_HEADER = DotName.createSimple(JmsHeader.class);
    static final DotName SEND_TO = DotName.createSimple(SendTo.class);
    static final DotName IDENTIFIER = DotName.createSimple(Identifier.class);
    static final DotName APPLICATION_SCOPED = DotName.createSimple(ApplicationScoped.class);

    static final DotName JMS_QUEUE = DotName.createSimple(JmsQueue.class);
    static final DotName JMS_TOPIC = DotName.createSimple(JmsTopic.class);

    static final DotName RUN_ON_VIRTUAL_THREAD = DotName.createSimple(
            "io.smallrye.common.annotation.RunOnVirtualThread");

    static final DotName JMS_CONVERTER = DotName.createSimple(JmsMessageConverter.class);
    static final DotName JACKSON_JMS_CONVERTER = DotName.createSimple(JacksonJmsMessageConverter.class);

    static final DotName JMS_RESPONSE = DotName.createSimple("io.quarkiverse.jms.JmsResponse");
    static final DotName SPRING_JMS_RESPONSE = DotName.createSimple("org.springframework.jms.listener.adapter.JmsResponse");

    private DotNames() {
    }
}
