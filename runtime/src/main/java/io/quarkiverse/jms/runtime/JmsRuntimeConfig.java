package io.quarkiverse.jms.runtime;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;

import io.quarkus.runtime.annotations.ConfigPhase;
import io.quarkus.runtime.annotations.ConfigRoot;
import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;

@ConfigMapping(prefix = "quarkus.jms")
@ConfigRoot(phase = ConfigPhase.RUN_TIME)
public interface JmsRuntimeConfig {

    /**
     * Listener defaults.
     */
    ListenerConfig listener();

    interface ListenerConfig {

        /**
         * Default acknowledge mode for listeners.
         */
        @WithDefault("auto")
        String acknowledgeMode();

        /**
         * Default number of concurrent consumers per listener.
         */
        @WithDefault("1")
        int concurrency();

        /**
         * Default maximum number of concurrent consumers per listener.
         * When set, listeners can scale up from {@code concurrency} to this value
         * under load, and scale back down when idle.
         */
        Optional<Integer> maxConcurrency();

        /**
         * How long a scaled-up consumer must be idle before it is removed.
         * Only applies when {@code maxConcurrency} is set.
         */
        @WithDefault("PT10S")
        Duration scaleDownIdleTimeout();

        /**
         * Maximum time to wait for in-flight messages to complete during shutdown.
         */
        @WithDefault("PT10S")
        Duration shutdownTimeout();

        /**
         * Number of reconnection attempts after a connection failure.
         * {@code -1} means unlimited retries (default), {@code 0} disables reconnection
         * (useful when the JMS client handles reconnection, e.g., via Artemis
         * {@code reconnectAttempts}).
         */
        @WithDefault("-1")
        int reconnectAttempts();

        /**
         * Initial delay before the first reconnection attempt.
         */
        @WithDefault("PT1S")
        Duration reconnectInterval();

        /**
         * Maximum delay between reconnection attempts.
         * The delay doubles after each failed attempt up to this value.
         */
        @WithDefault("PT30S")
        Duration reconnectMaxInterval();
    }

    /**
     * Connection and session pooling configuration.
     */
    PoolConfig pool();

    interface PoolConfig {

        /**
         * Whether to enable connection/session pooling. When enabled, a single shared
         * connection is used and sessions are pooled per acknowledge mode.
         * Automatically disabled when {@code quarkus-pooled-jms} is present.
         */
        @WithDefault("true")
        boolean enabled();

        /**
         * Maximum number of sessions per acknowledge mode. Under concurrent load,
         * callers block until a session becomes available when the pool is exhausted.
         */
        @WithDefault("500")
        int maxSessions();

        /**
         * Maximum time to wait for a pooled session to become available.
         * If the pool is exhausted and no session is returned within this
         * duration, a {@code JMSRuntimeException} is thrown. When empty,
         * callers block indefinitely.
         */
        Optional<Duration> borrowTimeout();

        /**
         * Maximum number of idle sessions kept in the pool per acknowledge mode.
         * When a session is returned and the idle count exceeds this value,
         * the excess sessions are closed immediately.
         */
        @WithDefault("10")
        int maxIdleSessions();

        /**
         * How long an idle session can sit in the pool before being evicted.
         * Eviction is performed lazily when sessions are returned to the pool.
         */
        @WithDefault("PT5M")
        Duration idleTimeout();

        /**
         * Whether to cache JMS consumers per destination and selector.
         * Cached consumers survive session pool round-trips, avoiding
         * consumer creation overhead on each receive.
         * <p>
         * Enable only when the broker's consumer prefetch is disabled
         * (e.g., Artemis {@code consumerWindowSize=0}), otherwise cached
         * consumers hoard prefetched messages while idle in the pool.
         */
        @WithDefault("false")
        boolean cacheConsumers();

        /**
         * Whether to automatically reconnect when the JMS connection fails.
         */
        @WithDefault("true")
        boolean reconnectOnException();
    }

    /**
     * Enable JMS 1.1 compatibility mode. When set, the extension wraps JMS 1.1
     * {@code Connection} and {@code Session} objects in a {@code JMSContext}
     * adapter, allowing providers that do not support the JMS 2.0 simplified
     * API to work (e.g., Amazon SQS, Oracle AQ).
     */
    @WithDefault("false")
    boolean jms11();

    /**
     * Default receive timeout for {@link io.quarkiverse.jms.JmsClient JmsClient} operations.
     * Used when no explicit timeout is provided.
     */
    @WithDefault("PT5S")
    Duration receiveTimeout();

    /**
     * Default client configuration applied to all JmsClient operations
     * unless overridden per-call.
     */
    ClientDefaults client();

    interface ClientDefaults {

        /**
         * Default delivery delay for outgoing messages, in milliseconds.
         */
        Optional<Long> deliveryDelay();

        /**
         * Default delivery mode: {@code persistent} or {@code non-persistent}.
         * Defaults to {@code persistent}.
         */
        @WithDefault("persistent")
        String deliveryMode();

        /**
         * Default message priority (0-9). {@code -1} means use JMS provider default.
         */
        @WithDefault("-1")
        int priority();

        /**
         * Default time-to-live for outgoing messages, in milliseconds.
         * {@code -1} means unlimited.
         */
        @WithDefault("-1")
        long timeToLive();
    }

    /**
     * Named destination configurations for {@link io.quarkiverse.jms.JmsQueue @JmsQueue}
     * and {@link io.quarkiverse.jms.JmsTopic @JmsTopic} injection.
     */
    Map<String, JmsDestinationConfig> destinations();

    /**
     * Message converter configuration.
     */
    ConverterConfig converter();

    interface ConverterConfig {

        /**
         * The JMS message property name used to store the payload type for deserialization.
         */
        @WithDefault("_type")
        String typeIdProperty();

        /**
         * Mappings from short type names to fully qualified class names.
         * When set, the short name is used in the type ID property instead of the FQCN.
         * Example: {@code quarkus.jms.converter.type-id-mappings.order=com.example.Order}
         */
        Map<String, String> typeIdMappings();
    }
}
