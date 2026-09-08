package io.quarkiverse.it.jms.ibmmq;

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
@WithTestResource(IbmMqTestResource.class)
public class JmsIbmMqIntegrationTest {

    @Test
    void sendAndReceive() {
        given().body("hello-ibmmq")
                .post("/jms/send/DEV.QUEUE.2")
                .then().statusCode(204);

        get("/jms/receive/DEV.QUEUE.2")
                .then()
                .statusCode(200)
                .body(is("hello-ibmmq"));
    }

    @Test
    void listenerReceivesMessages() {
        given().body("listener-ibmmq")
                .post("/jms/send/DEV.QUEUE.1")
                .then().statusCode(204);

        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            List<String> received = get("/jms/listener/received")
                    .as(new TypeRef<>() {
                    });
            assertThat(received).contains("listener-ibmmq");
        });
    }

    @Test
    void jmsClientXaCommitsBothJmsAndDb() {
        given().body("client-xa-msg")
                .post("/jms/xa/send-and-insert")
                .then().statusCode(204);

        List<String> dbMessages = get("/jms/xa/messages").as(new TypeRef<>() {
        });
        assertThat(dbMessages).contains("client-xa:client-xa-msg");

        String received = get("/jms/receive/DEV.QUEUE.3")
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
