package io.quarkiverse.it.jms;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;

@ApplicationScoped
public class ConnectorApp {

    private final CopyOnWriteArrayList<String> received = new CopyOnWriteArrayList<>();

    @Incoming("connector-channel")
    public void consume(String payload) {
        received.add(payload);
    }

    public List<String> getReceived() {
        return received;
    }
}
