package io.quarkiverse.jms;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Injects a JMS message property into a {@link JmsListener} method parameter.
 *
 * <pre>{@code
 * &#64;JmsListener("orders")
 * void process(Order order, @JmsHeader("correlationId") String correlationId) { ... }
 * }</pre>
 */
@Target(ElementType.PARAMETER)
@Retention(RetentionPolicy.RUNTIME)
public @interface JmsHeader {

    /**
     * The JMS message property name.
     */
    String value();
}
