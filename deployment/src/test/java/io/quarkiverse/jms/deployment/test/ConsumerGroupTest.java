package io.quarkiverse.jms.deployment.test;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

import io.quarkiverse.jms.runtime.ConsumerGroup;

public class ConsumerGroupTest {

    // --- canScaleUp ---

    @Test
    void canScaleUpWhenAllConsumersBusyAndBelowMax() {
        ConsumerGroup group = createGroup(1, 5);
        group.consumerCount.set(2);
        group.activeCount.set(2);

        assertThat(group.canScaleUp()).isTrue();
    }

    @Test
    void cannotScaleUpWhenNotAllConsumersBusy() {
        ConsumerGroup group = createGroup(1, 5);
        group.consumerCount.set(3);
        group.activeCount.set(1);

        assertThat(group.canScaleUp()).isFalse();
    }

    @Test
    void cannotScaleUpWhenAtMaxConcurrency() {
        ConsumerGroup group = createGroup(1, 5);
        group.consumerCount.set(5);
        group.activeCount.set(5);

        assertThat(group.canScaleUp()).isFalse();
    }

    @Test
    void cannotScaleUpWhenMinEqualsMax() {
        ConsumerGroup group = createGroup(3, 3);
        group.consumerCount.set(3);
        group.activeCount.set(3);

        assertThat(group.canScaleUp()).isFalse();
    }

    @Test
    void cannotScaleUpWhenNoActiveConsumers() {
        ConsumerGroup group = createGroup(1, 5);
        group.consumerCount.set(2);
        group.activeCount.set(0);

        assertThat(group.canScaleUp()).isFalse();
    }

    // --- tryScaleDown ---

    @Test
    void scaleDownSucceedsWhenAboveMin() {
        ConsumerGroup group = createGroup(1, 5);
        group.consumerCount.set(3);

        assertThat(group.tryScaleDown()).isTrue();
        assertThat(group.consumerCount.get()).isEqualTo(2);
    }

    @Test
    void scaleDownSucceedsDownToMin() {
        ConsumerGroup group = createGroup(2, 5);
        group.consumerCount.set(3);

        assertThat(group.tryScaleDown()).isTrue();
        assertThat(group.consumerCount.get()).isEqualTo(2);
    }

    @Test
    void scaleDownFailsWhenAtMin() {
        ConsumerGroup group = createGroup(2, 5);
        group.consumerCount.set(2);

        assertThat(group.tryScaleDown()).isFalse();
        assertThat(group.consumerCount.get()).isEqualTo(2);
    }

    @Test
    void scaleDownFailsWhenBelowMin() {
        ConsumerGroup group = createGroup(3, 5);
        group.consumerCount.set(3);

        assertThat(group.tryScaleDown()).isFalse();
        assertThat(group.consumerCount.get()).isEqualTo(3);
    }

    @Test
    void concurrentScaleDownNeverGoesBelowMin() throws Exception {
        ConsumerGroup group = createGroup(2, 10);
        group.consumerCount.set(8);

        int threads = 10;
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads);
        AtomicInteger successes = new AtomicInteger();

        for (int i = 0; i < threads; i++) {
            new Thread(() -> {
                try {
                    start.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                if (group.tryScaleDown()) {
                    successes.incrementAndGet();
                }
                done.countDown();
            }).start();
        }

        start.countDown();
        done.await();

        assertThat(group.consumerCount.get()).isGreaterThanOrEqualTo(2);
        assertThat(successes.get()).isEqualTo(6);
    }

    private static ConsumerGroup createGroup(int min, int max) {
        return new ConsumerGroup(null, null, null, null, null, null, 0, min, max, 1000, 3, -1, 1000, 30000, false);
    }
}
