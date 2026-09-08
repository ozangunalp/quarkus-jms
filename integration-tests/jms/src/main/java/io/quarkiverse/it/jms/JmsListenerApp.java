package io.quarkiverse.it.jms;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import jakarta.enterprise.context.ApplicationScoped;

import io.quarkiverse.jms.JmsListener;

@ApplicationScoped
public class JmsListenerApp {

    private final CopyOnWriteArrayList<String> received = new CopyOnWriteArrayList<>();

    @JmsListener("it-queue")
    public void onMessage(String message) {
        received.add(message);
    }

    public List<String> getReceived() {
        return received;
    }
}
