package io.quarkiverse.jms;

import jakarta.jms.DeliveryMode;

/**
 * JMS delivery mode — type-safe alternative to the raw {@link DeliveryMode} int constants.
 */
public enum JmsDeliveryMode {

    PERSISTENT(DeliveryMode.PERSISTENT),
    NON_PERSISTENT(DeliveryMode.NON_PERSISTENT);

    private final int value;

    JmsDeliveryMode(int value) {
        this.value = value;
    }

    /**
     * The JMS {@link DeliveryMode} int constant.
     */
    public int value() {
        return value;
    }
}
