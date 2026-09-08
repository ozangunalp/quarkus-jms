package io.quarkiverse.it.jms;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.JMSContext;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;

import io.quarkiverse.jms.JmsClient;
import io.quarkiverse.jms.JmsListenerRegistry;
import io.quarkiverse.jms.JmsQueue;
import io.quarkus.runtime.StartupEvent;

@Path("/jms/benchmark")
@Produces(MediaType.APPLICATION_JSON)
@ApplicationScoped
public class BenchmarkResource {

    @Inject
    JmsClient jms;

    @Inject
    ConnectionFactory connectionFactory;

    @Inject
    JmsListenerRegistry registry;

    void onStart(@Observes StartupEvent ev, JmsListenerRegistry registry) {
        registry.register("bench-programmatic-queue", String.class, msg -> {
        });
    }

    @GET
    @Path("/warmup")
    public void warmup() {
        for (int i = 0; i < 100; i++) {
            jms.queue("bench-warmup-queue").send("warmup-" + i);
        }
        JmsQueue<String> q = jms.queue("bench-warmup-queue", String.class);
        for (int i = 0; i < 100; i++) {
            q.receive();
        }
        try (JMSContext ctx = connectionFactory.createContext()) {
            var queue = ctx.createQueue("bench-warmup-raw");
            var producer = ctx.createProducer();
            for (int i = 0; i < 100; i++) {
                producer.send(queue, "warmup-" + i);
            }
            var consumer = ctx.createConsumer(queue);
            for (int i = 0; i < 100; i++) {
                consumer.receive(1000);
            }
        }
    }

    @GET
    @Path("/send/{count}")
    public Map<String, Object> benchmarkSend(@PathParam("count") int count) {
        JmsQueue<Object> queue = jms.queue("bench-send-queue");
        long start = System.nanoTime();
        for (int i = 0; i < count; i++) {
            queue.send("msg-" + i);
        }
        long elapsed = System.nanoTime() - start;
        double throughput = count / (elapsed / 1_000_000_000.0);
        return Map.of("count", count, "elapsedMs", elapsed / 1_000_000, "throughput", Math.round(throughput));
    }

    @GET
    @Path("/listener-preload/{queue}/{count}")
    public void preloadMessages(@PathParam("queue") String queueName, @PathParam("count") int count) {
        try (JMSContext ctx = connectionFactory.createContext()) {
            var queue = ctx.createQueue(queueName);
            var producer = ctx.createProducer();
            for (int i = 0; i < count; i++) {
                producer.send(queue, "msg-" + i);
            }
        }
    }

    @GET
    @Path("/listener-consume/{queue}/{count}")
    public Map<String, Object> benchmarkListenerConsume(
            @PathParam("queue") String queueName, @PathParam("count") int count,
            @QueryParam("concurrency") @DefaultValue("1") int concurrency)
            throws Exception {
        CountDownLatch latch = new CountDownLatch(count);
        long start = System.nanoTime();
        registry.register(
                JmsListenerRegistry.ListenerConfig.queue(queueName).concurrency(concurrency),
                String.class, msg -> latch.countDown());
        latch.await(120, TimeUnit.SECONDS);
        long elapsed = System.nanoTime() - start;
        int processed = count - (int) latch.getCount();
        double throughput = processed / (elapsed / 1_000_000_000.0);
        return Map.of("count", count, "processed", processed, "elapsedMs", elapsed / 1_000_000,
                "throughput", Math.round(throughput));
    }

    @GET
    @Path("/raw-send-reuse/{count}")
    public Map<String, Object> benchmarkRawSendReuse(@PathParam("count") int count) {
        long start = System.nanoTime();
        try (JMSContext ctx = connectionFactory.createContext()) {
            var queue = ctx.createQueue("bench-raw-send-reuse-queue");
            var producer = ctx.createProducer();
            for (int i = 0; i < count; i++) {
                producer.send(queue, "msg-" + i);
            }
        }
        long elapsed = System.nanoTime() - start;
        double throughput = count / (elapsed / 1_000_000_000.0);
        return Map.of("count", count, "elapsedMs", elapsed / 1_000_000, "throughput", Math.round(throughput));
    }

    @GET
    @Path("/raw-send-per-message/{count}")
    public Map<String, Object> benchmarkRawSendPerMessage(@PathParam("count") int count) {
        long start = System.nanoTime();
        for (int i = 0; i < count; i++) {
            try (JMSContext ctx = connectionFactory.createContext()) {
                ctx.createProducer().send(ctx.createQueue("bench-raw-send-pm-queue"), "msg-" + i);
            }
        }
        long elapsed = System.nanoTime() - start;
        double throughput = count / (elapsed / 1_000_000_000.0);
        return Map.of("count", count, "elapsedMs", elapsed / 1_000_000, "throughput", Math.round(throughput));
    }

    @GET
    @Path("/raw-receive-reuse/{count}")
    public Map<String, Object> benchmarkRawReceiveReuse(@PathParam("count") int count) {
        try (JMSContext ctx = connectionFactory.createContext()) {
            var queue = ctx.createQueue("bench-raw-recv-reuse-queue");
            var producer = ctx.createProducer();
            for (int i = 0; i < count; i++) {
                producer.send(queue, "msg-" + i);
            }
        }
        long start = System.nanoTime();
        int received = 0;
        try (JMSContext ctx = connectionFactory.createContext()) {
            var consumer = ctx.createConsumer(ctx.createQueue("bench-raw-recv-reuse-queue"));
            for (int i = 0; i < count; i++) {
                if (consumer.receive(5000) != null) {
                    received++;
                }
            }
        }
        long elapsed = System.nanoTime() - start;
        double throughput = received / (elapsed / 1_000_000_000.0);
        return Map.of("count", count, "received", received, "elapsedMs", elapsed / 1_000_000,
                "throughput", Math.round(throughput));
    }

    @GET
    @Path("/raw-receive-per-message/{count}")
    public Map<String, Object> benchmarkRawReceivePerMessage(@PathParam("count") int count) {
        try (JMSContext ctx = connectionFactory.createContext()) {
            var queue = ctx.createQueue("bench-raw-recv-pm-queue");
            var producer = ctx.createProducer();
            for (int i = 0; i < count; i++) {
                producer.send(queue, "msg-" + i);
            }
        }
        long start = System.nanoTime();
        int received = 0;
        for (int i = 0; i < count; i++) {
            try (JMSContext ctx = connectionFactory.createContext()) {
                var msg = ctx.createConsumer(ctx.createQueue("bench-raw-recv-pm-queue")).receive(5000);
                if (msg != null) {
                    received++;
                }
            }
        }
        long elapsed = System.nanoTime() - start;
        double throughput = received / (elapsed / 1_000_000_000.0);
        return Map.of("count", count, "received", received, "elapsedMs", elapsed / 1_000_000,
                "throughput", Math.round(throughput));
    }

    @GET
    @Path("/execute-receive/{count}")
    public Map<String, Object> benchmarkExecuteReceive(@PathParam("count") int count) {
        jms.queue("bench-exec-queue").execute(ctx -> {
            var queue = ctx.createQueue("bench-exec-queue");
            var producer = ctx.createProducer();
            for (int i = 0; i < count; i++) {
                producer.send(queue, "msg-" + i);
            }
            return null;
        });
        long start = System.nanoTime();
        int received = jms.queue("bench-exec-queue").<Integer> execute(ctx -> {
            var consumer = ctx.createConsumer(ctx.createQueue("bench-exec-queue"));
            int recv = 0;
            for (int i = 0; i < count; i++) {
                if (consumer.receive(5000) != null) {
                    recv++;
                }
            }
            return recv;
        });
        long elapsed = System.nanoTime() - start;
        double throughput = received / (elapsed / 1_000_000_000.0);
        return Map.of("count", count, "received", received, "elapsedMs", elapsed / 1_000_000,
                "throughput", Math.round(throughput));
    }

    @GET
    @Path("/receiveWith/{count}")
    public Map<String, Object> benchmarkReceiveWith(@PathParam("count") int count) {
        JmsQueue<String> queue = jms.queue("bench-receivewith-queue", String.class);
        for (int i = 0; i < count; i++) {
            queue.send("msg-" + i);
        }
        long start = System.nanoTime();
        int received = queue.receiveWith(receiver -> {
            int recv = 0;
            for (int i = 0; i < count; i++) {
                if (receiver.receive().isPresent()) {
                    recv++;
                }
            }
            return recv;
        });
        long elapsed = System.nanoTime() - start;
        double throughput = received / (elapsed / 1_000_000_000.0);
        return Map.of("count", count, "received", received, "elapsedMs", elapsed / 1_000_000,
                "throughput", Math.round(throughput));
    }
}
