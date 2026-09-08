package io.quarkiverse.spring.jms.runtime;

import jakarta.jms.Destination;
import jakarta.jms.JMSContext;
import jakarta.jms.JMSException;
import jakarta.jms.Session;

import org.springframework.jms.listener.adapter.JmsResponse;
import org.springframework.jms.support.destination.DestinationResolver;

public class SpringJmsResponseHelper implements DestinationResolver {

    private final JMSContext ctx;

    public SpringJmsResponseHelper(JMSContext ctx) {
        this.ctx = ctx;
    }

    @Override
    public Destination resolveDestinationName(Session session, String destinationName, boolean pubSubDomain)
            throws JMSException {
        return pubSubDomain ? ctx.createTopic(destinationName) : ctx.createQueue(destinationName);
    }

    public static Destination resolveDestination(JmsResponse<?> response, JMSContext ctx) {
        try {
            return response.resolveDestination(new SpringJmsResponseHelper(ctx), null);
        } catch (JMSException e) {
            throw new RuntimeException("Failed to resolve Spring JmsResponse destination", e);
        }
    }
}
