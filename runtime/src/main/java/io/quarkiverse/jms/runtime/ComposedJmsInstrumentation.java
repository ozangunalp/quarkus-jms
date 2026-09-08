package io.quarkiverse.jms.runtime;

import java.util.ArrayList;
import java.util.List;

import jakarta.jms.Message;

import org.jboss.logging.Logger;

class ComposedJmsInstrumentation implements JmsInstrumentation {

    private static final Logger LOG = Logger.getLogger(ComposedJmsInstrumentation.class);

    private final List<JmsInstrumentation> delegates;

    ComposedJmsInstrumentation(List<JmsInstrumentation> delegates) {
        this.delegates = delegates;
    }

    @Override
    public Scope onSend(String destination, boolean isTopic) {
        List<Scope> scopes = new ArrayList<>();
        for (JmsInstrumentation delegate : delegates) {
            try {
                scopes.add(delegate.onSend(destination, isTopic));
            } catch (Throwable t) {
                LOG.errorf(t, "Error in JmsInstrumentation delegate %s onSend for destination %s",
                        delegate.getClass().getName(), destination);
            }
        }
        return new ComposedScope(scopes);
    }

    @Override
    public Scope onReceive(String destination, boolean isTopic) {
        List<Scope> scopes = new ArrayList<>();
        for (JmsInstrumentation delegate : delegates) {
            try {
                scopes.add(delegate.onReceive(destination, isTopic));
            } catch (Throwable t) {
                LOG.errorf(t, "Error in JmsInstrumentation delegate %s onReceive for destination %s",
                        delegate.getClass().getName(), destination);
            }
        }
        return new ComposedScope(scopes);
    }

    @Override
    public Scope onProcess(String destination, boolean isTopic) {
        List<Scope> scopes = new ArrayList<>();
        for (JmsInstrumentation delegate : delegates) {
            try {
                scopes.add(delegate.onProcess(destination, isTopic));
            } catch (Throwable t) {
                LOG.errorf(t, "Error in JmsInstrumentation delegate %s onProcess for destination %s",
                        delegate.getClass().getName(), destination);
            }
        }
        return new ComposedScope(scopes);
    }

    record ComposedScope(List<Scope> scopes) implements Scope {
        @Override
        public void onMessage(Message message) {
            scopes.forEach(s -> s.onMessage(message));
        }

        @Override
        public void onError(Throwable t) {
            scopes.forEach(s -> s.onError(t));
        }

        @Override
        public void close() {
            scopes.forEach(Scope::close);
        }
    }

}
