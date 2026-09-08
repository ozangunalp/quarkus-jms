package io.quarkiverse.it.jms.ibmmq;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import io.quarkiverse.jms.JmsListener;

public class JmsListenerApp {

    private final List<String> received = new CopyOnWriteArrayList<>();

    @JmsListener("DEV.QUEUE.1")
    void onMessage(String message) {
        received.add(message);
    }

    public List<String> getReceived() {
        return received;
    }
}
