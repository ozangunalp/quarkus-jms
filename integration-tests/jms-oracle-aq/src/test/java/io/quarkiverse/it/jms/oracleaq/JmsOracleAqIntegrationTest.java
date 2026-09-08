package io.quarkiverse.it.jms.oracleaq;

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
@WithTestResource(OracleAqTestResource.class)
public class JmsOracleAqIntegrationTest {

    @Test
    void sendAndReceive() {
        given().body("hello-oracle-aq")
                .post("/jms/send/SYSTEM.TEST_QUEUE")
                .then().statusCode(204);

        get("/jms/receive/SYSTEM.TEST_QUEUE")
                .then()
                .statusCode(200)
                .body(is("hello-oracle-aq"));
    }

    @Test
    void listenerReceivesMessages() {
        given().body("listener-oracle-aq")
                .post("/jms/send/SYSTEM.LISTENER_QUEUE")
                .then().statusCode(204);

        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            List<String> received = get("/jms/listener/received")
                    .as(new TypeRef<>() {
                    });
            assertThat(received).contains("listener-oracle-aq");
        });
    }
}
