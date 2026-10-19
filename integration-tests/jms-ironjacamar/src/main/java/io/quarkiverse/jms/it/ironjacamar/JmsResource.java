package io.quarkiverse.jms.it.ironjacamar;

import java.util.List;

import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

import io.quarkiverse.jms.JmsClient;

@Path("/jms")
@Produces(MediaType.APPLICATION_JSON)
public class JmsResource {

    @Inject
    JmsClient jms;

    @Inject
    JmsListenerApp listenerApp;

    @POST
    @Path("/send")
    public void send(String message) {
        jms.queue("test-queue", String.class).send(message);
    }

    @GET
    @Path("/received")
    public List<String> received() {
        return listenerApp.getReceived();
    }
}
