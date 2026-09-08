package io.quarkiverse.it.jms.oracleaq;

import java.util.List;

import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

import io.quarkiverse.jms.JmsClient;

@Path("/jms")
@Produces(MediaType.APPLICATION_JSON)
public class JmsResource {

    @Inject
    JmsClient jmsClient;

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

}
