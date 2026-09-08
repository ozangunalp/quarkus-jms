package io.quarkiverse.it.jms;

import java.util.List;
import java.util.Map;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;

import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;

@Path("/jms/traces")
public class TracingResource {

    @Inject
    InMemorySpanExporter exporter;

    @GET
    public List<Map<String, String>> getSpans() {
        return exporter.getFinishedSpanItems()
                .stream()
                .filter(sd -> !sd.getName().contains("export") && !sd.getName().contains("reset"))
                .map(s -> Map.of(
                        "name", s.getName(),
                        "kind", s.getKind().name(),
                        "traceId", s.getSpanContext().getTraceId(),
                        "parentTraceId",
                        s.getParentSpanContext().getTraceId()))
                .toList();
    }

    @DELETE
    public void reset() {
        exporter.reset();
    }

    @ApplicationScoped
    static class InMemorySpanExporterProducer {
        @Produces
        @Singleton
        InMemorySpanExporter inMemorySpanExporter() {
            return InMemorySpanExporter.create();
        }
    }
}
