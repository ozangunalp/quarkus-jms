package io.quarkiverse.it.jms;

import static io.restassured.RestAssured.get;
import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.restassured.common.mapper.TypeRef;

@QuarkusTest
@TestProfile(FailoverTestProfile.class)
public class JmsBrokerFailoverTest {

    @Test
    @Timeout(60)
    void listenerRecoversAfterNetworkCut() throws Exception {
        given().body("before-cut")
                .post("/jms/send/it-queue")
                .then().statusCode(204);

        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            List<String> received = get("/jms/listener/received")
                    .as(new TypeRef<>() {
                    });
            assertThat(received).contains("before-cut");
        });

        ArtemisTestResource.cutConnection();
        Thread.sleep(10000);
        ArtemisTestResource.restoreConnection();

        await().atMost(30, TimeUnit.SECONDS).pollInterval(1, TimeUnit.SECONDS).untilAsserted(() -> {
            given().body("after-cut")
                    .post("/jms/send/it-queue")
                    .then().statusCode(204);
            List<String> received = get("/jms/listener/received")
                    .as(new TypeRef<>() {
                    });
            assertThat(received).contains("after-cut");
        });
    }
}
