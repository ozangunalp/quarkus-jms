package io.quarkiverse.jms.runtime;

import java.util.List;

import jakarta.jms.Message;

/**
 * SPI for instrumenting JMS operations (tracing, metrics, etc.).
 * Implementations are discovered via CDI and called for each operation.
 * <p>
 * The scope lifecycle is:
 * <ol>
 * <li>{@code onSend/onReceive/onProcess} — starts the scope (timing, span creation)</li>
 * <li>{@code scope.onMessage(message)} — called when the message is available (trace context injection/extraction)</li>
 * <li>{@code scope.onError(t)} — called on failure (optional)</li>
 * <li>{@code scope.close()} — ends the scope (records duration, ends span)</li>
 * </ol>
 */
public interface JmsInstrumentation {

    JmsInstrumentation NOOP = new JmsInstrumentation() {
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
            return Scope.NOOP;
        }
    };

    interface Scope extends AutoCloseable {

        Scope NOOP = new Scope() {
            @Override
            public void onMessage(Message message) {
            }

            @Override
            public void onError(Throwable t) {
            }

            @Override
            public void close() {
            }
        };

        void onMessage(Message message);

        void onError(Throwable t);

        @Override
        void close();
    }

    Scope onSend(String destination, boolean isTopic);

    Scope onReceive(String destination, boolean isTopic);

    Scope onProcess(String destination, boolean isTopic);

    static JmsInstrumentation create(List<JmsInstrumentation> instrumentations) {
        if (instrumentations == null || instrumentations.isEmpty()) {
            return NOOP;
        }
        if (instrumentations.size() == 1) {
            return instrumentations.get(0);
        }
        return new ComposedJmsInstrumentation(instrumentations);
    }
}
