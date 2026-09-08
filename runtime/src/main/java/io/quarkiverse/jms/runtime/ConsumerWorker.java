package io.quarkiverse.jms.runtime;

import java.util.concurrent.atomic.AtomicInteger;

import jakarta.jms.Destination;
import jakarta.jms.JMSConsumer;
import jakarta.jms.JMSContext;
import jakarta.jms.Message;
import jakarta.jms.XAJMSContext;
import jakarta.transaction.Transaction;

import org.jboss.logging.Logger;

import io.smallrye.common.vertx.VertxContext;
import io.vertx.core.Context;
import io.vertx.core.internal.ContextInternal;

class ConsumerWorker {

    private static final Logger LOG = Logger.getLogger(ConsumerWorker.class);

    private final ConsumerGroup group;
    private final ConsumerContext consumerContext;
    private final boolean scaledUp;
    private final AtomicInteger consecutiveIdle = new AtomicInteger();
    private JMSContext context;
    private JMSConsumer consumer;

    ConsumerWorker(ConsumerGroup group, ConsumerContext consumerContext, boolean scaledUp) {
        this.group = group;
        this.consumerContext = consumerContext;
        this.scaledUp = scaledUp;
    }

    void start() {
        int consumerId = group.consumerCount.getAndIncrement();

        if (!group.isXa()) {
            this.context = group.cf.createContext(group.sessionMode);
            consumerContext.trackContext(context);
            this.consumer = createConsumer(context);
        }

        submitPollCycle();

        JmsListenerMetadata metadata = group.metadata;
        LOG.infof("Started %sJMS listener: %s.%s on %s '%s' (consumer %d/%s)",
                group.isXa() ? "XA " : "",
                shortClassName(metadata.getBeanClassName()),
                metadata.getMethodName(),
                metadata.isTopic() ? "topic" : "queue",
                metadata.getDestination(),
                consumerId + 1,
                group.maxConcurrency > group.minConcurrency
                        ? group.minConcurrency + "-" + group.maxConcurrency
                        : String.valueOf(group.minConcurrency));
    }

    private void submitPollCycle() {
        if (group.runOnVirtualThread) {
            group.virtualThreadExecutor.execute(this::runPollCycle);
        } else {
            group.worker.executeBlocking(() -> {
                runPoll();
                return null;
            }, false).onComplete(ar -> handlePollResult(ar.cause()));
        }
    }

    private void runPollCycle() {
        try {
            runPoll();
            handlePollResult(null);
        } catch (Throwable e) {
            handlePollResult(e);
        }
    }

    private void runPoll() {
        if (group.isXa()) {
            pollXa();
        } else {
            pollStandard();
        }
    }

    private void handlePollResult(Throwable failure) {
        if (consumerContext.isStopped()) {
            return;
        }
        if (failure != null) {
            if (group.reconnectAttempts != 0) {
                LOG.warnf(failure, "Connection lost for listener on %s, reconnecting...",
                        group.metadata.getDestination());
                reconnect();
            } else {
                LOG.debugf(failure, "Poll cycle failed for listener on %s, resubmitting...",
                        group.metadata.getDestination());
                submitPollCycle();
            }
            return;
        }
        if (shouldScaleDown()) {
            scaleDown();
            return;
        }
        submitPollCycle();
    }

    /**
     * Avoids re-entering executeBlocking between messages by draining remaining messages
     * via receiveNoWait() after the blocking receive returns a message.
     */
    private void pollStandard() {
        Message received = consumer.receive(group.receiveTimeoutMs);
        if (received != null) {
            group.activeCount.incrementAndGet();
            try {
                if (group.canScaleUp()) {
                    LOG.debugf("Scaling up consumer for %s (consumers=%d)",
                            group.metadata.getDestination(), group.consumerCount.get() + 1);
                    consumerContext.spawnConsumer(group, true);
                }
                processMessage(received);
                Message next;
                while (!consumerContext.isStopped() && (next = consumer.receiveNoWait()) != null) {
                    processMessage(next);
                }
            } finally {
                group.activeCount.decrementAndGet();
            }
            consecutiveIdle.set(0);
        } else {
            consecutiveIdle.incrementAndGet();
        }
    }

    private void pollXa() {
        try (XAJMSContext xaContext = group.xaConnectionFactory.createXAContext()) {
            group.transactionManager.begin();
            Transaction tx = group.transactionManager.getTransaction();
            tx.enlistResource(xaContext.getXAResource());

            JMSConsumer xaConsumer = createConsumer(xaContext);
            Message received = xaConsumer.receive(group.receiveTimeoutMs);

            if (received != null) {
                group.activeCount.incrementAndGet();
                JmsInstrumentation.Scope scope = consumerContext.instrumentation()
                        .onProcess(group.metadata.getDestination(), group.metadata.isTopic());
                scope.onMessage(received);
                try {
                    if (group.canScaleUp()) {
                        LOG.debugf("Scaling up XA consumer for %s (consumers=%d)",
                                group.metadata.getDestination(), group.consumerCount.get() + 1);
                        consumerContext.spawnConsumer(group, true);
                    }
                    group.invoker.invoke(received, xaContext);
                    group.transactionManager.commit();
                } catch (Throwable e) {
                    scope.onError(e);
                    consumerContext.onListenerError(xaContext, group.metadata.getDestination(), e, received);
                    group.transactionManager.rollback();
                } finally {
                    scope.close();
                    group.activeCount.decrementAndGet();
                }
                consecutiveIdle.set(0);
            } else {
                group.transactionManager.rollback();
                consecutiveIdle.incrementAndGet();
            }
            xaConsumer.close();
        } catch (Exception e) {
            try {
                group.transactionManager.rollback();
            } catch (Exception rollbackEx) {
                e.addSuppressed(rollbackEx);
            }
            LOG.errorf(e, "Error in XA poll cycle for %s", group.metadata.getDestination());
            consecutiveIdle.incrementAndGet();
        }
    }

    private void processMessage(Message message) {
        Context dc = VertxContext.createNewDuplicatedContext();
        if (dc instanceof ContextInternal ci) {
            ContextInternal prev = ci.beginDispatch();
            try {
                invokeListener(message);
            } finally {
                ci.endDispatch(prev);
            }
        } else {
            invokeListener(message);
        }
    }

    private void invokeListener(Message message) {
        JmsInstrumentation.Scope scope = consumerContext.instrumentation()
                .onProcess(group.metadata.getDestination(), group.metadata.isTopic());
        scope.onMessage(message);
        try {
            group.invoker.invoke(message, context);
            if (group.isTransacted()) {
                context.commit();
            } else if (group.isClientAcknowledge()) {
                message.acknowledge();
            }
        } catch (Throwable e) {
            scope.onError(e);
            consumerContext.onListenerError(context, group.metadata.getDestination(), e, message);
            if (group.isTransacted()) {
                try {
                    context.rollback();
                } catch (Exception rollbackEx) {
                    LOG.error("Error rolling back JMS session", rollbackEx);
                }
            } else if (group.isClientAcknowledge()) {
                try {
                    context.recover();
                } catch (Exception recoverEx) {
                    LOG.error("Error recovering JMS session", recoverEx);
                }
            }
        } finally {
            scope.close();
        }
    }

    private JMSConsumer createConsumer(JMSContext ctx) {
        JmsListenerMetadata metadata = group.metadata;
        Destination destination = metadata.isTopic()
                ? ctx.createTopic(metadata.getDestination())
                : ctx.createQueue(metadata.getDestination());
        String selector = metadata.getSelector().isEmpty() ? null : metadata.getSelector();
        if (metadata.isTopic() && !metadata.getSubscription().isEmpty()) {
            return ctx.createDurableConsumer(
                    ctx.createTopic(metadata.getDestination()),
                    metadata.getSubscription(), selector, false);
        }
        return ctx.createConsumer(destination, selector);
    }

    private boolean shouldScaleDown() {
        return scaledUp
                && consecutiveIdle.get() >= group.idleScaleDownPolls
                && group.consumerCount.get() > group.minConcurrency;
    }

    private void scaleDown() {
        if (group.tryScaleDown()) {
            LOG.debugf("Scaling down consumer for %s (consumers=%d)",
                    group.metadata.getDestination(), group.consumerCount.get());
            close();
        }
    }

    private void reconnect() {
        close();
        group.consumerCount.decrementAndGet();
        group.worker.executeBlocking(() -> {
            if (!group.isXa()) {
                consumerContext.reconnectWithBackoff(group);
            }
            return null;
        }, false).onComplete(ar -> {
            if (consumerContext.isStopped()) {
                return;
            }
            group.worker.executeBlocking(() -> {
                consumerContext.spawnConsumer(group, scaledUp);
                return null;
            }, false);
        });
    }

    private void close() {
        if (consumer != null) {
            try {
                consumer.close();
            } catch (Exception e) {
                LOG.warnf(e, "Error closing consumer on %s", group.metadata.getDestination());
            }
        }
        if (context != null) {
            try {
                context.close();
            } catch (Exception e) {
                LOG.warnf(e, "Error closing context on %s", group.metadata.getDestination());
            }
        }
    }

    private static String shortClassName(String fqcn) {
        int dot = fqcn.lastIndexOf('.');
        return dot >= 0 ? fqcn.substring(dot + 1) : fqcn;
    }
}
