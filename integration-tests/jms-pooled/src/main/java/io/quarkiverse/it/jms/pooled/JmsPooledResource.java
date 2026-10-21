package io.quarkiverse.it.jms.pooled;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;

import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;

import org.messaginghub.pooled.jms.JmsPoolConnectionFactory;

import io.quarkiverse.jms.JmsClient;
import io.quarkiverse.jms.JmsListener;
import io.quarkiverse.jms.runtime.JmsConnectionFactoryResolver;
import io.quarkus.arc.ClientProxy;

@Path("/jms")
public class JmsPooledResource {

    @Inject
    JmsClient jms;

    @Inject
    JmsConnectionFactoryResolver resolver;

    private final List<String> received = new CopyOnWriteArrayList<>();

    @POST
    @Path("/send/{queue}")
    public void send(@PathParam("queue") String queue, String body) {
        jms.queue(queue).send(body);
    }

    @GET
    @Path("/receive/{queue}")
    public String receive(@PathParam("queue") String queue) {
        Optional<String> msg = jms.queue(queue, String.class).receive();
        return msg.orElse(null);
    }

    @GET
    @Path("/listener/received")
    public List<String> listenerReceived() {
        return received;
    }

    @GET
    @Path("/connection-factory-type")
    public String connectionFactoryType() {
        return ClientProxy.unwrap(resolver.resolve(null)).getClass().getName();
    }

    @GET
    @Path("/wrapped-factory-type")
    public String wrappedFactoryType() {
        Object cf = ClientProxy.unwrap(resolver.resolve(null));
        if (cf instanceof JmsPoolConnectionFactory pooled) {
            Object wrapped = pooled.getConnectionFactory();
            return ClientProxy.unwrap(wrapped).getClass().getName();
        }
        return cf.getClass().getName();
    }

    @JmsListener("pooled-listener-queue")
    void onMessage(String body) {
        received.add(body);
    }
}
