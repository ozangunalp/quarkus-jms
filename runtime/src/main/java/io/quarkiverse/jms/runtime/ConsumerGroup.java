package io.quarkiverse.jms.runtime;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import jakarta.jms.ConnectionFactory;
import jakarta.jms.Session;
import jakarta.jms.XAConnectionFactory;
import jakarta.transaction.TransactionManager;

import io.quarkus.virtual.threads.VirtualThreadsRecorder;
import io.vertx.core.WorkerExecutor;

public class ConsumerGroup {

    final WorkerExecutor worker;
    final ConnectionFactory cf;
    final XAConnectionFactory xaConnectionFactory;
    final TransactionManager transactionManager;
    final JmsListenerMetadata metadata;
    final JmsListenerInvoker invoker;
    final int sessionMode;
    final int minConcurrency;
    final int maxConcurrency;
    final long receiveTimeoutMs;
    final int idleScaleDownPolls;
    final int reconnectAttempts;
    final long reconnectIntervalMs;
    final long reconnectMaxIntervalMs;
    final boolean runOnVirtualThread;
    final ExecutorService virtualThreadExecutor;
    public final AtomicInteger consumerCount = new AtomicInteger();
    public final AtomicInteger activeCount = new AtomicInteger();

    public ConsumerGroup(WorkerExecutor worker,
            ConnectionFactory cf,
            XAConnectionFactory xaConnectionFactory,
            TransactionManager transactionManager,
            JmsListenerMetadata metadata,
            JmsListenerInvoker invoker,
            int sessionMode,
            int minConcurrency,
            int maxConcurrency,
            long receiveTimeoutMs,
            int idleScaleDownPolls,
            int reconnectAttempts,
            long reconnectIntervalMs,
            long reconnectMaxIntervalMs,
            boolean runOnVirtualThread) {
        this.worker = worker;
        this.cf = cf;
        this.xaConnectionFactory = xaConnectionFactory;
        this.transactionManager = transactionManager;
        this.metadata = metadata;
        this.invoker = invoker;
        this.sessionMode = sessionMode;
        this.minConcurrency = minConcurrency;
        this.maxConcurrency = maxConcurrency;
        this.receiveTimeoutMs = receiveTimeoutMs;
        this.idleScaleDownPolls = idleScaleDownPolls;
        this.reconnectAttempts = reconnectAttempts;
        this.reconnectIntervalMs = reconnectIntervalMs;
        this.reconnectMaxIntervalMs = reconnectMaxIntervalMs;
        this.runOnVirtualThread = runOnVirtualThread;
        this.virtualThreadExecutor = runOnVirtualThread ? VirtualThreadsRecorder.getCurrent() : null;
    }

    boolean isXa() {
        return xaConnectionFactory != null;
    }

    boolean isTransacted() {
        return !isXa() && sessionMode == Session.SESSION_TRANSACTED;
    }

    boolean isClientAcknowledge() {
        return sessionMode == Session.CLIENT_ACKNOWLEDGE;
    }

    public boolean canScaleUp() {
        return maxConcurrency > minConcurrency
                && activeCount.get() >= consumerCount.get()
                && consumerCount.get() < maxConcurrency;
    }

    public boolean tryScaleDown() {
        if (consumerCount.decrementAndGet() >= minConcurrency) {
            return true;
        }
        consumerCount.incrementAndGet();
        return false;
    }
}
