package io.quarkiverse.it.jms;

import static io.restassured.RestAssured.get;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import io.quarkus.test.junit.QuarkusTest;
import io.restassured.RestAssured;
import io.restassured.common.mapper.TypeRef;
import io.restassured.config.HttpClientConfig;

@QuarkusTest
@Disabled("Benchmark — run manually with -Dtest=JmsThroughputTest")
public class JmsThroughputTest {

    private static final int MESSAGE_COUNT = 2000;
    private static final int HIGH_THROUGHPUT_COUNT = 5000;
    private static volatile boolean warmedUp;

    @BeforeEach
    void warmup() {
        if (!warmedUp) {
            RestAssured.config = RestAssured.config().httpClient(
                    HttpClientConfig.httpClientConfig()
                            .setParam("http.socket.timeout", 120000)
                            .setParam("http.connection.timeout", 120000));
            get("/jms/benchmark/warmup").then().statusCode(204);
            warmedUp = true;
        }
    }

    @Test
    void sendThroughput() {
        Map<String, Object> result = get("/jms/benchmark/send/" + MESSAGE_COUNT)
                .as(new TypeRef<>() {
                });
        printResult("JmsClient Send", result);
    }

    @Test
    void rawSendReuseThroughput() {
        Map<String, Object> result = get("/jms/benchmark/raw-send-reuse/" + MESSAGE_COUNT)
                .as(new TypeRef<>() {
                });
        printResult("Raw Send (reuse ctx)", result);
    }

    @Test
    void rawSendPerMessageThroughput() {
        Map<String, Object> result = get("/jms/benchmark/raw-send-per-message/" + MESSAGE_COUNT)
                .as(new TypeRef<>() {
                });
        printResult("Raw Send (new ctx/msg)", result);
    }

    @Test
    void rawReceiveReuseThroughput() {
        Map<String, Object> result = get("/jms/benchmark/raw-receive-reuse/" + HIGH_THROUGHPUT_COUNT)
                .as(new TypeRef<>() {
                });
        int received = ((Number) result.get("received")).intValue();
        assertThat(received).isEqualTo(HIGH_THROUGHPUT_COUNT);
        printResult("Raw Receive (reuse ctx)", result);
    }

    @Test
    void rawReceivePerMessageThroughput() {
        Map<String, Object> result = get("/jms/benchmark/raw-receive-per-message/" + MESSAGE_COUNT)
                .as(new TypeRef<>() {
                });
        int received = ((Number) result.get("received")).intValue();
        assertThat(received).isEqualTo(MESSAGE_COUNT);
        printResult("Raw Receive (new ctx/msg)", result);
    }

    @Test
    void listenerThroughput() {
        String queue = "bench-listener-" + System.nanoTime();
        get("/jms/benchmark/listener-preload/" + queue + "/" + HIGH_THROUGHPUT_COUNT)
                .then().statusCode(204);

        Map<String, Object> result = get("/jms/benchmark/listener-consume/" + queue + "/" + HIGH_THROUGHPUT_COUNT)
                .as(new TypeRef<>() {
                });
        int processed = ((Number) result.get("processed")).intValue();
        assertThat(processed).isEqualTo(HIGH_THROUGHPUT_COUNT);
        printResult("@JmsListener (concurrency=1)", result);
    }

    @Test
    void listenerConcurrency2Throughput() {
        String queue = "bench-listener-c2-" + System.nanoTime();
        get("/jms/benchmark/listener-preload/" + queue + "/" + HIGH_THROUGHPUT_COUNT)
                .then().statusCode(204);

        Map<String, Object> result = get("/jms/benchmark/listener-consume/" + queue + "/" + HIGH_THROUGHPUT_COUNT
                + "?concurrency=2")
                .as(new TypeRef<>() {
                });
        int processed = ((Number) result.get("processed")).intValue();
        assertThat(processed).isEqualTo(HIGH_THROUGHPUT_COUNT);
        printResult("@JmsListener (concurrency=2)", result);
    }

    @Test
    void executeReceiveThroughput() {
        Map<String, Object> result = get("/jms/benchmark/execute-receive/" + HIGH_THROUGHPUT_COUNT)
                .as(new TypeRef<>() {
                });
        int received = ((Number) result.get("received")).intValue();
        assertThat(received).isEqualTo(HIGH_THROUGHPUT_COUNT);
        printResult("JmsClient execute receive", result);
    }

    @Test
    void receiveWithThroughput() {
        Map<String, Object> result = get("/jms/benchmark/receiveWith/" + HIGH_THROUGHPUT_COUNT)
                .as(new TypeRef<>() {
                });
        int received = ((Number) result.get("received")).intValue();
        assertThat(received).isEqualTo(HIGH_THROUGHPUT_COUNT);
        printResult("JmsClient receiveWith", result);
    }

    private void printResult(String label, Map<String, Object> result) {
        int throughput = ((Number) result.get("throughput")).intValue();
        long elapsedMs = ((Number) result.get("elapsedMs")).longValue();
        int count = ((Number) result.get("count")).intValue();
        System.out.printf("%-30s %d messages in %5dms — %,d msg/s%n", label, count, elapsedMs, throughput);
    }
}
