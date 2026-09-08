package io.quarkiverse.jms.runtime.instrumentation;

import java.util.Collections;
import java.util.List;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.jms.JMSException;
import jakarta.jms.Message;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.propagation.TextMapGetter;
import io.opentelemetry.context.propagation.TextMapPropagator;
import io.opentelemetry.context.propagation.TextMapSetter;
import io.quarkiverse.jms.runtime.JmsInstrumentation;

@ApplicationScoped
public class JmsTracingInstrumentation implements JmsInstrumentation {

    private static final String INSTRUMENTATION_NAME = "io.quarkiverse.jms";

    private static final TextMapSetter<Message> SETTER = (carrier, key, value) -> {
        try {
            carrier.setStringProperty(key.replace('-', '_'), value);
        } catch (JMSException e) {
            // ignore
        }
    };

    private static final TextMapGetter<Message> GETTER = new TextMapGetter<>() {
        @Override
        public Iterable<String> keys(Message carrier) {
            try {
                return Collections.list(carrier.getPropertyNames());
            } catch (JMSException e) {
                return List.of();
            }
        }

        @Override
        public String get(Message carrier, String key) {
            try {
                return carrier.getStringProperty(key.replace('-', '_'));
            } catch (JMSException e) {
                return null;
            }
        }
    };

    @Override
    public Scope onSend(String destination, boolean isTopic) {
        Span span = tracer().spanBuilder(destination + " publish")
                .setSpanKind(SpanKind.PRODUCER)
                .setAttribute("messaging.system", "jms")
                .setAttribute("messaging.destination.name", destination)
                .setAttribute("messaging.destination.kind", isTopic ? "topic" : "queue")
                .startSpan();
        io.opentelemetry.context.Scope otelScope = span.makeCurrent();
        return new SendTracingScope(span, otelScope, propagator());
    }

    @Override
    public Scope onReceive(String destination, boolean isTopic) {
        return new ReceiveTracingScope(destination, isTopic);
    }

    @Override
    public Scope onProcess(String destination, boolean isTopic) {
        return new ProcessTracingScope(destination, isTopic);
    }

    private Tracer tracer() {
        return GlobalOpenTelemetry.getTracer(INSTRUMENTATION_NAME);
    }

    private TextMapPropagator propagator() {
        return GlobalOpenTelemetry.getPropagators().getTextMapPropagator();
    }

    private record SendTracingScope(Span span, io.opentelemetry.context.Scope otelScope,
            TextMapPropagator propagator) implements Scope {
        @Override
        public void onMessage(Message message) {
            propagator.inject(Context.current(), message, SETTER);
        }

        @Override
        public void onError(Throwable t) {
            span.recordException(t);
            span.setStatus(StatusCode.ERROR, t.getMessage());
        }

        @Override
        public void close() {
            span.end();
            otelScope.close();
        }
    }

    private class ReceiveTracingScope implements Scope {
        private final String destination;
        private final boolean isTopic;
        private Span span;
        private io.opentelemetry.context.Scope otelScope;

        ReceiveTracingScope(String destination, boolean isTopic) {
            this.destination = destination;
            this.isTopic = isTopic;
        }

        @Override
        public void onMessage(Message message) {
            if (message == null) {
                return;
            }
            Context extracted = propagator().extract(Context.current(), message, GETTER);
            span = tracer().spanBuilder(destination + " receive")
                    .setSpanKind(SpanKind.CONSUMER)
                    .setParent(extracted)
                    .setAttribute("messaging.system", "jms")
                    .setAttribute("messaging.destination.name", destination)
                    .setAttribute("messaging.destination.kind", isTopic ? "topic" : "queue")
                    .startSpan();
            otelScope = span.makeCurrent();
        }

        @Override
        public void onError(Throwable t) {
            if (span != null) {
                span.recordException(t);
                span.setStatus(StatusCode.ERROR, t.getMessage());
            }
        }

        @Override
        public void close() {
            if (span != null) {
                span.end();
                otelScope.close();
            }
        }
    }

    private class ProcessTracingScope implements Scope {
        private final String destination;
        private final boolean isTopic;
        private Span span;
        private io.opentelemetry.context.Scope otelScope;

        ProcessTracingScope(String destination, boolean isTopic) {
            this.destination = destination;
            this.isTopic = isTopic;
        }

        @Override
        public void onMessage(Message message) {
            Context extracted = propagator().extract(Context.current(), message, GETTER);
            SpanBuilder builder = tracer().spanBuilder(destination + " process")
                    .setSpanKind(SpanKind.CONSUMER)
                    .setParent(extracted)
                    .setAttribute("messaging.system", "jms")
                    .setAttribute("messaging.destination.name", destination)
                    .setAttribute("messaging.destination.kind", isTopic ? "topic" : "queue")
                    .setAttribute("messaging.operation.type", "process");
            try {
                String messageId = message.getJMSMessageID();
                if (messageId != null) {
                    builder.setAttribute("messaging.message.id", messageId);
                }
            } catch (JMSException e) {
                // ignore
            }
            span = builder.startSpan();
            otelScope = span.makeCurrent();
        }

        @Override
        public void onError(Throwable t) {
            if (span != null) {
                span.recordException(t);
                span.setStatus(StatusCode.ERROR, t.getMessage());
            }
        }

        @Override
        public void close() {
            if (span != null) {
                span.end();
                otelScope.close();
            }
        }
    }
}
