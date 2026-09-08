package io.quarkiverse.it.jms.sqs;

import static io.restassured.RestAssured.get;
import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.is;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.restassured.common.mapper.TypeRef;

@QuarkusTest
@WithTestResource(ElasticMqTestResource.class)
public class JmsSqsIntegrationTest {

    @Test
    void sendAndReceive() {
        given().body("hello-sqs")
                .post("/jms/send/test-queue")
                .then().statusCode(204);

        get("/jms/receive/test-queue")
                .then()
                .statusCode(200)
                .body(is("hello-sqs"));
    }

    @Test
    void listenerReceivesMessages() {
        given().body("listener-sqs")
                .post("/jms/send/listener-queue")
                .then().statusCode(204);

        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            List<String> received = get("/jms/listener/received")
                    .as(new TypeRef<>() {
                    });
            assertThat(received).contains("listener-sqs");
        });
    }
}
