package io.quarkiverse.it.jms.oracleaq;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import io.quarkiverse.jms.JmsListener;

public class JmsListenerApp {

    private final List<String> received = new CopyOnWriteArrayList<>();

    @JmsListener("SYSTEM.LISTENER_QUEUE")
    void onMessage(String message) {
        received.add(message);
    }

    public List<String> getReceived() {
        return received;
    }
}
