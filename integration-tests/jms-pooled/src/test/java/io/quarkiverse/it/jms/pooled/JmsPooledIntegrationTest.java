package io.quarkiverse.it.jms.pooled;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.is;

import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import io.quarkus.test.junit.QuarkusTest;

@QuarkusTest
public class JmsPooledIntegrationTest {

    @Test
    void connectionFactoryIsPooled() {
        String type = given().get("/jms/connection-factory-type")
                .then().statusCode(200)
                .extract().body().asString();
        assertThat(type).contains("JmsPoolConnectionFactory");
    }

    @Test
    void builtInSessionPoolingIsDisabled() {
        String outerType = given().get("/jms/connection-factory-type")
                .then().statusCode(200)
                .extract().body().asString();
        String wrappedType = given().get("/jms/wrapped-factory-type")
                .then().statusCode(200)
                .extract().body().asString();
        assertThat(outerType).doesNotContain("SessionPoolingConnectionFactory");
        assertThat(wrappedType).doesNotContain("SessionPoolingConnectionFactory");
    }

    @Test
    void sendAndReceiveWithPooledJms() {
        given().body("pooled-hello")
                .post("/jms/send/pooled-queue")
                .then().statusCode(204);

        given().get("/jms/receive/pooled-queue")
                .then().statusCode(200).body(is("pooled-hello"));
    }

    @Test
    void listenerReceivesWithPooledJms() {
        given().body("pooled-listener-msg")
                .post("/jms/send/pooled-listener-queue")
                .then().statusCode(204);

        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            var received = given().get("/jms/listener/received")
                    .then().statusCode(200)
                    .extract().jsonPath().getList(".", String.class);
            assertThat(received).contains("pooled-listener-msg");
        });
    }

    @Test
    void multipleMessagesWithPooledConnections() {
        for (int i = 0; i < 10; i++) {
            given().body("batch-" + i)
                    .post("/jms/send/pooled-batch")
                    .then().statusCode(204);
        }

        for (int i = 0; i < 10; i++) {
            given().get("/jms/receive/pooled-batch")
                    .then().statusCode(200).body(is("batch-" + i));
        }
    }
}
