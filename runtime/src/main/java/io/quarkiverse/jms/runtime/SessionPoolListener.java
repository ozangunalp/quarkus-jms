package io.quarkiverse.jms.runtime;

/**
 * Listener for session pool lifecycle events. Implementations are
 * discovered via CDI and called by {@link SessionPoolingConnectionFactory}.
 */
public interface SessionPoolListener {

    SessionPoolListener NOOP = new SessionPoolListener() {
    };

    default void onPoolCreated(String factoryId, int maxSessions) {
    }

    default void onSessionCreated(String factoryId) {
    }

    default void onSessionClosed(String factoryId) {
    }

    default void onSessionBorrowed(String factoryId) {
    }

    default void onSessionReturned(String factoryId) {
    }

    default void onBorrowTimeout(String factoryId) {
    }
}
