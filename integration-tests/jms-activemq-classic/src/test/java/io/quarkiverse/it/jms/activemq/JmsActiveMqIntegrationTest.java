package io.quarkiverse.it.jms.activemq;

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
@WithTestResource(ActiveMqTestResource.class)
public class JmsActiveMqIntegrationTest {

    @Test
    void sendAndReceive() {
        given().body("hello-activemq")
                .post("/jms/send/test-queue")
                .then().statusCode(204);

        get("/jms/receive/test-queue")
                .then()
                .statusCode(200)
                .body(is("hello-activemq"));
    }

    @Test
    void listenerReceivesMessages() {
        given().body("listener-activemq")
                .post("/jms/send/listener-queue")
                .then().statusCode(204);

        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            List<String> received = get("/jms/listener/received")
                    .as(new TypeRef<>() {
                    });
            assertThat(received).contains("listener-activemq");
        });
    }

    @Test
    void jmsListenerXaCommitsBothJmsAndDb() {
        given().body("listener-xa-activemq")
                .post("/jms/send/xa-listener-queue")
                .then().statusCode(204);

        await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> {
            List<String> xaReceived = get("/jms/listener/xa-received").as(new TypeRef<>() {
            });
            assertThat(xaReceived).contains("listener-xa-activemq");
        });

        List<String> dbMessages = get("/jms/xa/messages").as(new TypeRef<>() {
        });
        assertThat(dbMessages).contains("listener-xa:listener-xa-activemq");
    }

    @Test
    void jmsClientXaCommitsBothJmsAndDb() {
        given().body("client-xa-msg")
                .post("/jms/xa/send-and-insert")
                .then().statusCode(204);

        List<String> dbMessages = get("/jms/xa/messages").as(new TypeRef<>() {
        });
        assertThat(dbMessages).contains("client-xa:client-xa-msg");

        String received = get("/jms/receive/xa-commit-queue")
                .then().statusCode(200).extract().asString();
        assertThat(received).isEqualTo("client-xa-msg");
    }

    @Test
    void jmsClientXaRollbackUndoesBoth() {
        given().body("client-rollback")
                .post("/jms/xa/send-and-rollback")
                .then().statusCode(500);

        List<String> dbMessages = get("/jms/xa/messages").as(new TypeRef<>() {
        });
        assertThat(dbMessages).doesNotContain("should-not-persist-client:client-rollback");
    }
}
