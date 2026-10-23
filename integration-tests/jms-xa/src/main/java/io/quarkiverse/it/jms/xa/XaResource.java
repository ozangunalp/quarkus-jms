package io.quarkiverse.it.jms.xa;

import java.util.List;

import jakarta.inject.Inject;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.JMSContext;
import jakarta.persistence.EntityManager;
import jakarta.transaction.Transactional;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

@Path("/xa")
@Produces(MediaType.APPLICATION_JSON)
public class XaResource {

    @Inject
    ConnectionFactory connectionFactory;

    @Inject
    XaProcessor processor;

    @Inject
    XaConcurrentProcessor concurrentProcessor;

    @Inject
    XaChainedListenerProcessor chainedProcessor;

    @Inject
    XaListenerConcurrentProcessor listenerConcurrentProcessor;

    @Inject
    EntityManager entityManager;

    @POST
    @Path("/send")
    public void send(String message) {
        try (JMSContext context = connectionFactory.createContext()) {
            context.createProducer().send(context.createQueue("xa-queue"), message);
        }
    }

    @POST
    @Path("/send-to-client-queue")
    public void sendToClientQueue(String message) {
        try (JMSContext context = connectionFactory.createContext()) {
            context.createProducer().send(context.createQueue("xa-client-queue"), message);
        }
    }

    @POST
    @Path("/send-to-rollback-queue")
    public void sendToRollbackQueue(String message) {
        try (JMSContext context = connectionFactory.createContext()) {
            context.createProducer().send(context.createQueue("xa-rollback-queue"), message);
        }
    }

    @POST
    @Path("/send-to-process-queue")
    public void sendToProcessQueue(String message) {
        try (JMSContext context = connectionFactory.createContext()) {
            context.createProducer().send(context.createQueue("process-input-queue"), message);
        }
    }

    @GET
    @Path("/receive-from-process-output")
    public String receiveFromProcessOutput() {
        try (JMSContext context = connectionFactory.createContext()) {
            jakarta.jms.Message msg = context.createConsumer(
                    context.createQueue("process-output-queue")).receive(5000);
            if (msg == null) {
                return null;
            }
            return msg.getBody(String.class);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @POST
    @Path("/send-to-listener-queue")
    public void sendToListenerQueue(String message) {
        try (JMSContext context = connectionFactory.createContext()) {
            context.createProducer().send(context.createQueue("xa-listener-queue"), message);
        }
    }

    @POST
    @Path("/send-to-listener-rollback-queue")
    public void sendToListenerRollbackQueue(String message) {
        try (JMSContext context = connectionFactory.createContext()) {
            context.createProducer().send(context.createQueue("xa-listener-rollback-queue"), message);
        }
    }

    @POST
    @Path("/send-to-concurrent-queue")
    public void sendToConcurrentQueue(String message) {
        try (JMSContext context = connectionFactory.createContext()) {
            context.createProducer().send(context.createQueue("xa-concurrent-queue"), message);
        }
    }

    @GET
    @Path("/concurrent/processed")
    public List<String> concurrentProcessed() {
        return concurrentProcessor.getProcessed();
    }

    @GET
    @Path("/concurrent/max-inflight")
    @Produces(MediaType.TEXT_PLAIN)
    public int concurrentMaxInflight() {
        return concurrentProcessor.getMaxInflight();
    }

    @POST
    @Path("/send-to-chain-stage-a")
    public void sendToChainStageA(String message) {
        try (JMSContext context = connectionFactory.createContext()) {
            context.createProducer().send(context.createQueue("chain-stage-a"), message);
        }
    }

    @POST
    @Path("/send-to-chain-stage-a-fail")
    public void sendToChainStageAFail(String message) {
        try (JMSContext context = connectionFactory.createContext()) {
            context.createProducer().send(context.createQueue("chain-stage-a-fail"), message);
        }
    }

    @GET
    @Path("/chain/stage-a")
    public List<String> chainStageA() {
        return chainedProcessor.getStageAProcessed();
    }

    @GET
    @Path("/chain/stage-b")
    public List<String> chainStageB() {
        return chainedProcessor.getStageBProcessed();
    }

    @POST
    @Path("/send-to-connector-rollback-queue")
    public void sendToConnectorRollbackQueue(String message) {
        try (JMSContext context = connectionFactory.createContext()) {
            context.createProducer().send(context.createQueue("xa-connector-rollback-queue"), message);
        }
    }

    @POST
    @Path("/send-to-listener-concurrent-queue")
    public void sendToListenerConcurrentQueue(String message) {
        try (JMSContext context = connectionFactory.createContext()) {
            context.createProducer().send(context.createQueue("xa-listener-concurrent-queue"), message);
        }
    }

    @GET
    @Path("/listener-concurrent/processed")
    public List<String> listenerConcurrentProcessed() {
        return listenerConcurrentProcessor.getProcessed();
    }

    @GET
    @Path("/listener-concurrent/max-inflight")
    @Produces(MediaType.TEXT_PLAIN)
    public int listenerConcurrentMaxInflight() {
        return listenerConcurrentProcessor.getMaxInflight();
    }

    @POST
    @Path("/send-to-hibernate-queue")
    public void sendToHibernateQueue(String message) {
        try (JMSContext context = connectionFactory.createContext()) {
            context.createProducer().send(context.createQueue("xa-hibernate-queue"), message);
        }
    }

    @POST
    @Path("/send-to-hibernate-rollback-queue")
    public void sendToHibernateRollbackQueue(String message) {
        try (JMSContext context = connectionFactory.createContext()) {
            context.createProducer().send(context.createQueue("xa-hibernate-rollback-queue"), message);
        }
    }

    @POST
    @Path("/send-to-batch-queue")
    public void sendToBatchQueue(String message) {
        try (JMSContext context = connectionFactory.createContext()) {
            context.createProducer().send(context.createQueue("xa-batch-queue"), message);
        }
    }

    @GET
    @Path("/hibernate/messages")
    @Transactional
    @SuppressWarnings("unchecked")
    public List<String> hibernateMessages() {
        return entityManager.createQuery("SELECT m.payload FROM XaMessageEntity m ORDER BY m.id")
                .getResultList();
    }

    @GET
    @Path("/messages")
    public List<String> messages() {
        return processor.getMessages();
    }
}
