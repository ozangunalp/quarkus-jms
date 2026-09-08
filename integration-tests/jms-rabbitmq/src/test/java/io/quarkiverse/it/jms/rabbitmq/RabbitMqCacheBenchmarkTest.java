package io.quarkiverse.it.jms.rabbitmq;

import static io.restassured.RestAssured.get;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.restassured.common.mapper.TypeRef;

@QuarkusTest
@WithTestResource(RabbitMqTestResource.class)
@Disabled("Benchmark — run manually")
public class RabbitMqCacheBenchmarkTest {

    static final int COUNT = 500;

    @Test
    void perCallReceiveThroughput() {
        Map<String, Object> result = get("/jms/benchmark/per-call-receive/" + COUNT)
                .as(new TypeRef<>() {
                });
        int received = ((Number) result.get("received")).intValue();
        assertThat(received).isEqualTo(COUNT);
        int throughput = ((Number) result.get("throughput")).intValue();
        long elapsedMs = ((Number) result.get("elapsedMs")).longValue();
        System.out.printf("RabbitMQ per-call receive: %d msgs in %dms — %,d msg/s%n",
                COUNT, elapsedMs, throughput);
    }

    @Test
    void executeReceiveThroughput() {
        Map<String, Object> result = get("/jms/benchmark/execute-receive/" + COUNT)
                .as(new TypeRef<>() {
                });
        int received = ((Number) result.get("received")).intValue();
        assertThat(received).isEqualTo(COUNT);
        int throughput = ((Number) result.get("throughput")).intValue();
        long elapsedMs = ((Number) result.get("elapsedMs")).longValue();
        System.out.printf("RabbitMQ execute receive:  %d msgs in %dms — %,d msg/s%n",
                COUNT, elapsedMs, throughput);
    }
}
