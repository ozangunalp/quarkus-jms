package io.quarkiverse.jms;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * When placed on a {@link JmsListener} method that returns a non-void value,
 * the return value is converted and sent to the specified destination.
 * If no destination is specified, the reply is sent to the {@code JMSReplyTo}
 * header of the incoming message.
 *
 * <pre>{@code
 * @JmsListener("requests")
 * @SendTo("responses")
 * OrderStatus process(Order order) {
 *     return new OrderStatus(order.id(), "PROCESSED");
 * }
 * }</pre>
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface SendTo {

    /**
     * The destination to send the reply to. When empty, uses the incoming
     * message's {@code JMSReplyTo} header.
     */
    String value() default "";

    /**
     * Whether the destination is a topic. Defaults to {@code false} (queue).
     */
    boolean topic() default false;
}
