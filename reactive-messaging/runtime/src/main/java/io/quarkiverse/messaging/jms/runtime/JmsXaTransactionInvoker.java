package io.quarkiverse.messaging.jms.runtime;

import java.util.Arrays;

import io.smallrye.reactive.messaging.Invoker;
import io.smallrye.reactive.messaging.jms.JmsXaTransactionMetadata;

/**
 * Custom invoker that resumes a suspended XA transaction before invoking
 * the bean method and suspends it after. This ensures the {@code @Incoming}
 * method runs with the XA transaction active, regardless of which thread
 * it executes on ({@code @Blocking} or {@code @NonBlocking}).
 * <p>
 * The {@link JmsXaTransactionMetadata} is passed as a synthetic parameter
 * (last argument), extracted from the message metadata by the framework.
 */
public abstract class JmsXaTransactionInvoker implements Invoker {

    private static final int SYNTHETIC_PARAM_COUNT = 1;

    protected abstract Object invokeBean(Object... args);

    @Override
    public Object invoke(Object... args) {
        JmsXaTransactionMetadata txMeta = (JmsXaTransactionMetadata) args[args.length - 1];
        Object[] beanArgs = Arrays.copyOf(args, args.length - SYNTHETIC_PARAM_COUNT);

        if (txMeta != null) {
            txMeta.resume();
        }
        try {
            return invokeBean(beanArgs);
        } finally {
            if (txMeta != null) {
                txMeta.suspend();
            }
        }
    }
}
