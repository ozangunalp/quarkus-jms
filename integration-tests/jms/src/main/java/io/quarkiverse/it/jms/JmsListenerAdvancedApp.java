package io.quarkiverse.it.jms;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.jms.JMSContext;
import jakarta.jms.Message;

import io.quarkiverse.jms.JmsHeader;
import io.quarkiverse.jms.JmsListener;
import io.quarkiverse.jms.JmsListenerErrorHandler;
import io.quarkiverse.jms.JmsListenerRegistry;
import io.quarkiverse.jms.JmsResponse;
import io.quarkiverse.jms.SendTo;
import io.quarkus.runtime.StartupEvent;

@ApplicationScoped
public class JmsListenerAdvancedApp implements JmsListenerErrorHandler {

    private final CopyOnWriteArrayList<String> headerResults = new CopyOnWriteArrayList<>();
    private final CopyOnWriteArrayList<String> scalingResults = new CopyOnWriteArrayList<>();
    private final CopyOnWriteArrayList<String> programmaticReceived = new CopyOnWriteArrayList<>();
    private final CopyOnWriteArrayList<String> errorHandlerErrors = new CopyOnWriteArrayList<>();
    private final AtomicInteger maxInflight = new AtomicInteger();
    private final AtomicInteger inflight = new AtomicInteger();

    @JmsListener("header-queue")
    public void onHeaderMessage(String body, @JmsHeader("priority") int priority,
            @JmsHeader("urgent") boolean urgent, @JmsHeader("category") String category) {
        headerResults.add(body + ":priority=" + priority + ":urgent=" + urgent + ":category=" + category);
    }

    @JmsListener(value = "scaling-queue", concurrency = 1, maxConcurrency = 4)
    public void onScalingMessage(String body) {
        int current = inflight.incrementAndGet();
        maxInflight.updateAndGet(max -> Math.max(max, current));
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            inflight.decrementAndGet();
            scalingResults.add(body);
        }
    }

    @JmsListener("reply-queue")
    @SendTo("reply-output-queue")
    public String onReplyMessage(String body) {
        return "reply:" + body;
    }

    @JmsListener("dynamic-reply-queue")
    public JmsResponse<String> onDynamicReply(String body) {
        String target = body.startsWith("vip:") ? "vip-output-queue" : "regular-output-queue";
        return JmsResponse.forQueue("processed:" + body, target);
    }

    @JmsListener("error-trigger-queue")
    public void onErrorTrigger(String body) {
        throw new RuntimeException("intentional error: " + body);
    }

    void onStart(@Observes StartupEvent ev, JmsListenerRegistry registry) {
        registry.register("programmatic-it-queue", String.class, programmaticReceived::add);
    }

    @Override
    public void handleError(JMSContext context, Message message, String destination, Throwable error) {
        errorHandlerErrors.add(destination + ":" + error.getMessage());
    }

    public List<String> getProgrammaticReceived() {
        return programmaticReceived;
    }

    public List<String> getErrorHandlerErrors() {
        return errorHandlerErrors;
    }

    public List<String> getHeaderResults() {
        return headerResults;
    }

    public List<String> getScalingResults() {
        return scalingResults;
    }

    public int getMaxInflight() {
        return maxInflight.get();
    }
}
