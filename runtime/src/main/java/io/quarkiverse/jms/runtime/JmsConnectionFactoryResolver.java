package io.quarkiverse.jms.runtime;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Default;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.XAConnectionFactory;

import io.smallrye.common.annotation.Identifier;

/**
 * Resolves and caches {@link ConnectionFactory} beans from the CDI container,
 * supporting named instances via {@link Identifier}.
 * When a wrapper is set (e.g., from {@code quarkus-pooled-jms}),
 * it is applied to every resolved factory.
 * <p>
 * Results are cached per identifier — subsequent calls with the same
 * identifier return the cached factory without re-querying CDI.
 */
@Singleton
public class JmsConnectionFactoryResolver {

    @Inject
    @Any
    Instance<ConnectionFactory> connectionFactories;

    @Inject
    @Any
    Instance<XAConnectionFactory> xaFactories;

    private volatile Function<ConnectionFactory, Object> wrapper;
    private final ConcurrentHashMap<String, ConnectionFactory> cfCache = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Optional<XAConnectionFactory>> xaCache = new ConcurrentHashMap<>();

    public void setWrapper(Function<ConnectionFactory, Object> wrapper) {
        this.wrapper = wrapper;
    }

    /**
     * Resolve a {@link ConnectionFactory} by identifier, applying the external
     * wrapper (e.g., pooled-jms) if one is configured. Results are cached.
     */
    public ConnectionFactory resolve(String identifier) {
        String key = key(identifier);
        return cfCache.computeIfAbsent(key, k -> {
            ConnectionFactory factory = select(connectionFactories, identifier).get();
            if (wrapper != null) {
                return (ConnectionFactory) wrapper.apply(factory);
            }
            return factory;
        });
    }

    /**
     * Resolve as {@link XAConnectionFactory} if an XA-capable factory bean
     * exists for the given identifier. Results are cached.
     *
     * @return the XA factory, or {@code null} if not XA-capable
     */
    public XAConnectionFactory resolveXA(String identifier) {
        String key = key(identifier);
        return xaCache.computeIfAbsent(key, k -> {
            Instance<XAConnectionFactory> instance = select(xaFactories, identifier);
            if (instance.isResolvable()) {
                return Optional.of(instance.get());
            }
            return Optional.empty();
        }).orElse(null);
    }

    private static String key(String identifier) {
        return identifier != null && !identifier.isEmpty() ? identifier : "__default__";
    }

    private <T> Instance<T> select(Instance<T> instances, String identifier) {
        if (identifier == null || identifier.isEmpty()) {
            return instances.select(Default.Literal.INSTANCE);
        }
        return instances.select(Identifier.Literal.of(identifier));
    }
}
