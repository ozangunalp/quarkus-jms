package io.quarkiverse.it.jms.rabbitmq;

import java.util.List;
import java.util.Map;

import jakarta.inject.Inject;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.JMSContext;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

import io.quarkiverse.jms.JmsClient;
import io.quarkiverse.jms.JmsQueue;

@Path("/jms")
@Produces(MediaType.APPLICATION_JSON)
public class JmsResource {

    @Inject
    JmsClient jmsClient;

    @Inject
    ConnectionFactory connectionFactory;

    @Inject
    JmsListenerApp listenerApp;

    @POST
    @Path("/send/{queue}")
    public void send(@PathParam("queue") String queue, String message) {
        jmsClient.queue(queue).send(message);
    }

    @GET
    @Path("/receive/{queue}")
    public String receive(@PathParam("queue") String queue) {
        return jmsClient.queue(queue)
                .receive(String.class)
                .orElse(null);
    }

    @GET
    @Path("/listener/received")
    public List<String> listenerReceived() {
        return listenerApp.getReceived();
    }

    @GET
    @Path("/benchmark/per-call-receive/{count}")
    public Map<String, Object> benchmarkPerCallReceive(@PathParam("count") int count) {
        try (JMSContext ctx = connectionFactory.createContext()) {
            var queue = ctx.createQueue("bench-per-call");
            var producer = ctx.createProducer();
            for (int i = 0; i < count; i++) {
                producer.send(queue, "msg-" + i);
            }
        }
        long start = System.nanoTime();
        int received = 0;
        JmsQueue<String> queue = jmsClient.queue("bench-per-call", String.class);
        for (int i = 0; i < count; i++) {
            if (queue.receive().isPresent()) {
                received++;
            }
        }
        long elapsed = System.nanoTime() - start;
        double throughput = received / (elapsed / 1_000_000_000.0);
        return Map.of("count", count, "received", received,
                "elapsedMs", elapsed / 1_000_000, "throughput", Math.round(throughput));
    }

    @GET
    @Path("/benchmark/execute-receive/{count}")
    public Map<String, Object> benchmarkExecuteReceive(@PathParam("count") int count) {
        try (JMSContext ctx = connectionFactory.createContext()) {
            var queue = ctx.createQueue("bench-execute");
            var producer = ctx.createProducer();
            for (int i = 0; i < count; i++) {
                producer.send(queue, "msg-" + i);
            }
        }
        long start = System.nanoTime();
        int received = jmsClient.queue("bench-execute").<Integer> execute(ctx -> {
            int recv = 0;
            try (var consumer = ctx.createConsumer(ctx.createQueue("bench-execute"))) {
                for (int i = 0; i < count; i++) {
                    if (consumer.receive(5000) != null) {
                        recv++;
                    }
                }
            }
            return recv;
        });
        long elapsed = System.nanoTime() - start;
        double throughput = received / (elapsed / 1_000_000_000.0);
        return Map.of("count", count, "received", received,
                "elapsedMs", elapsed / 1_000_000, "throughput", Math.round(throughput));
    }
}
