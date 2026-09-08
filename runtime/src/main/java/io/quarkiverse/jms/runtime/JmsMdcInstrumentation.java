package io.quarkiverse.jms.runtime;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.jms.JMSException;
import jakarta.jms.Message;

import org.jboss.logging.MDC;

@ApplicationScoped
public class JmsMdcInstrumentation implements JmsInstrumentation {

    private static final String MDC_MESSAGE_ID = "jms.messageId";
    private static final String MDC_CORRELATION_ID = "jms.correlationId";

    @Override
    public Scope onSend(String destination, boolean isTopic) {
        return Scope.NOOP;
    }

    @Override
    public Scope onReceive(String destination, boolean isTopic) {
        return Scope.NOOP;
    }

    @Override
    public Scope onProcess(String destination, boolean isTopic) {
        return new MdcScope();
    }

    private static class MdcScope implements Scope {

        @Override
        public void onMessage(Message message) {
            try {
                String messageId = message.getJMSMessageID();
                if (messageId != null) {
                    MDC.put(MDC_MESSAGE_ID, messageId);
                }
                String correlationId = message.getJMSCorrelationID();
                if (correlationId != null) {
                    MDC.put(MDC_CORRELATION_ID, correlationId);
                }
            } catch (JMSException e) {
                // ignore
            }
        }

        @Override
        public void onError(Throwable t) {
        }

        @Override
        public void close() {
            MDC.remove(MDC_MESSAGE_ID);
            MDC.remove(MDC_CORRELATION_ID);
        }
    }
}
