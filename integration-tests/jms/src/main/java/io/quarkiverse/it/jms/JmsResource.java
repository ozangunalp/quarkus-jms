package io.quarkiverse.it.jms;

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
import io.smallrye.common.annotation.Identifier;

@Path("/jms")
@Produces(MediaType.APPLICATION_JSON)
public class JmsResource {

    @Inject
    JmsClient jmsClient;

    @Inject
    ConnectionFactory connectionFactory;

    @Inject
    @Identifier("injected-queue")
    JmsQueue<String> injectedQueue;

    @Inject
    @Identifier("filtered-queue")
    JmsQueue<String> filteredQueue;

    @Inject
    JmsListenerApp listenerApp;

    @Inject
    JmsListenerAdvancedApp advancedApp;

    @Inject
    ConnectorApp connectorApp;

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
    @Path("/connector/received")
    public List<String> connectorReceived() {
        return connectorApp.getReceived();
    }

    @POST
    @Path("/send-with-headers/{queue}")
    public void sendWithHeaders(@PathParam("queue") String queue, String message) {
        try (JMSContext ctx = connectionFactory.createContext()) {
            var jmsMsg = ctx.createTextMessage(message);
            jmsMsg.setIntProperty("priority", 5);
            jmsMsg.setBooleanProperty("urgent", true);
            jmsMsg.setStringProperty("category", "test");
            ctx.createProducer().send(ctx.createQueue(queue), jmsMsg);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @GET
    @Path("/header/results")
    public List<String> headerResults() {
        return advancedApp.getHeaderResults();
    }

    @POST
    @Path("/injected/send")
    public void injectedSend(String message) {
        injectedQueue.send(message);
    }

    @GET
    @Path("/injected/receive")
    public String injectedReceive() {
        return injectedQueue.receive().orElse(null);
    }

    @POST
    @Path("/filtered/send")
    public void filteredSend(String message) {
        try (JMSContext ctx = connectionFactory.createContext()) {
            var msg = ctx.createTextMessage(message);
            msg.setStringProperty("category", "vip");
            ctx.createProducer().send(ctx.createQueue("filtered-queue"), msg);
            var regularMsg = ctx.createTextMessage("should-be-filtered");
            regularMsg.setStringProperty("category", "regular");
            ctx.createProducer().send(ctx.createQueue("filtered-queue"), regularMsg);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @GET
    @Path("/filtered/receive")
    public String filteredReceive() {
        return filteredQueue
                .receive()
                .orElse(null);
    }

    @GET
    @Path("/scaling/results")
    public List<String> scalingResults() {
        return advancedApp.getScalingResults();
    }

    @GET
    @Path("/scaling/max-inflight")
    @Produces(MediaType.TEXT_PLAIN)
    public int scalingMaxInflight() {
        return advancedApp.getMaxInflight();
    }

    @GET
    @Path("/browse/{queue}")
    public List<String> browse(@PathParam("queue") String queue) {
        return jmsClient.queue(queue, String.class).browse();
    }

    @POST
    @Path("/reset-connection")
    public void resetConnection() {
        jmsClient.resetConnection();
    }

    @POST
    @Path("/send-with-props/{queue}")
    public void sendWithProps(@PathParam("queue") String queue, String message) {
        jmsClient.queue(queue).message(message)
                .priority(7)
                .correlationId("corr-123")
                .property("source", "integration-test")
                .send();
    }

    @GET
    @Path("/receive-with-props/{queue}")
    public Map<String, String> receiveWithProps(@PathParam("queue") String queue) {
        return jmsClient.queue(queue)
                .receiveMessage(String.class)
                .map(rm -> {
                    try {
                        return Map.of(
                                "body", rm.payload(),
                                "correlationId", rm.message().getJMSCorrelationID(),
                                "priority", String.valueOf(rm.message().getJMSPriority()),
                                "source", rm.message().getStringProperty("source"));
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                })
                .orElse(Map.of());
    }

    @POST
    @Path("/send-pojo/{queue}")
    @jakarta.ws.rs.Consumes(MediaType.APPLICATION_JSON)
    public void sendPojo(@PathParam("queue") String queue, OrderDto order) {
        jmsClient.queue(queue).send(order);
    }

    @GET
    @Path("/receive-pojo/{queue}")
    public OrderDto receivePojo(@PathParam("queue") String queue) {
        return jmsClient.queue(queue, OrderDto.class)
                .receive()
                .orElse(null);
    }

    @GET
    @Path("/programmatic/received")
    public List<String> programmaticReceived() {
        return advancedApp.getProgrammaticReceived();
    }

    @GET
    @Path("/error-handler/errors")
    public List<String> errorHandlerErrors() {
        return advancedApp.getErrorHandlerErrors();
    }

    public record OrderDto(String id, String product, int quantity) {
    }
}
