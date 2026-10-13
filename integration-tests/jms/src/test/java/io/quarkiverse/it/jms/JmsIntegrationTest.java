package io.quarkiverse.it.jms;

import static io.restassured.RestAssured.delete;
import static io.restassured.RestAssured.get;
import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.is;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import io.quarkus.test.junit.QuarkusTest;
import io.restassured.common.mapper.TypeRef;

/**
 * Full integration test for the Quarkus JMS extension.
 * Requires DevServices to auto-start an Artemis broker.
 */
@QuarkusTest
public class JmsIntegrationTest {

    @Test
    void jmsClientSendAndReceive() {
        given().body("hello-integration")
                .post("/jms/send/send-recv-queue")
                .then().statusCode(204);

        get("/jms/receive/send-recv-queue")
                .then()
                .statusCode(200)
                .body(is("hello-integration"));
    }

    @Test
    void jmsListenerReceivesMessages() {
        given().body("listener-msg")
                .post("/jms/send/it-queue")
                .then().statusCode(204);

        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            List<String> received = get("/jms/listener/received")
                    .as(new TypeRef<>() {
                    });
            assertThat(received).contains("listener-msg");
        });
    }

    @Test
    void jmsListenerTypedHeaders() {
        given().body("header-test")
                .post("/jms/send-with-headers/header-queue")
                .then().statusCode(204);

        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            List<String> results = get("/jms/header/results").as(new TypeRef<>() {
            });
            assertThat(results).contains("header-test:priority=5:urgent=true:category=test");
        });
    }

    @Test
    void jmsResponseDynamicReplyDestination() {
        given().body("vip:order1").post("/jms/send/dynamic-reply-queue").then().statusCode(204);
        given().body("regular:order2").post("/jms/send/dynamic-reply-queue").then().statusCode(204);

        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            String vip = get("/jms/receive/vip-output-queue")
                    .then().statusCode(200).extract().asString();
            assertThat(vip).isEqualTo("processed:vip:order1");
        });

        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            String regular = get("/jms/receive/regular-output-queue")
                    .then().statusCode(200).extract().asString();
            assertThat(regular).isEqualTo("processed:regular:order2");
        });
    }

    @Test
    void jmsListenerMaxConcurrencyScalesUp() {
        for (int i = 1; i <= 8; i++) {
            given().body("scale-" + i)
                    .post("/jms/send/scaling-queue")
                    .then().statusCode(204);
        }

        await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> {
            List<String> results = get("/jms/scaling/results").as(new TypeRef<>() {
            });
            assertThat(results).hasSize(8);
        });

        int maxInflight = Integer.parseInt(
                get("/jms/scaling/max-inflight").then().extract().asString());
        assertThat(maxInflight).as("Expected scaling up beyond initial concurrency of 1").isGreaterThan(1);
    }

    @Test
    void jmsListenerSendToReply() {
        given().body("request-msg")
                .post("/jms/send/reply-queue")
                .then().statusCode(204);

        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            String reply = get("/jms/receive/reply-output-queue")
                    .then().statusCode(200)
                    .extract().asString();
            assertThat(reply).isEqualTo("reply:request-msg");
        });
    }

    @Test
    void injectedDestinationSendAndReceive() {
        given().body("injected-msg")
                .post("/jms/injected/send")
                .then().statusCode(204);

        get("/jms/injected/receive")
                .then()
                .statusCode(200)
                .body(is("injected-msg"));
    }

    @Test
    void injectedDestinationWithSelectorConfig() {
        // Send both VIP and regular messages to the same queue
        given().body("vip-msg")
                .post("/jms/filtered/send")
                .then().statusCode(204);

        // The filtered-queue destination has selector=category='vip' from config
        // so it should only receive the VIP message
        get("/jms/filtered/receive")
                .then()
                .statusCode(200)
                .body(is("vip-msg"));
    }

    @Test
    void jmsClientBrowseQueue() {
        given().body("browse-1").post("/jms/send/browse-it-queue").then().statusCode(204);
        given().body("browse-2").post("/jms/send/browse-it-queue").then().statusCode(204);

        await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
            List<String> browsed = get("/jms/browse/browse-it-queue")
                    .as(new TypeRef<>() {
                    });
            assertThat(browsed).hasSize(2);
        });

        // Browse again — messages should still be there (not consumed)
        List<String> browsedAgain = get("/jms/browse/browse-it-queue")
                .as(new TypeRef<>() {
                });
        assertThat(browsedAgain).hasSize(2);
    }

    @Test
    void sendAndReceiveWithPropertiesPreserved() {
        given().body("props-msg")
                .post("/jms/send-with-props/props-queue")
                .then().statusCode(204);

        Map<String, String> result = get("/jms/receive-with-props/props-queue")
                .as(new TypeRef<>() {
                });

        assertThat(result.get("body")).isEqualTo("props-msg");
        assertThat(result.get("correlationId")).isEqualTo("corr-123");
        assertThat(result.get("priority")).isEqualTo("7");
        assertThat(result.get("source")).isEqualTo("integration-test");
    }

    @Test
    void jsonPojoSendAndReceive() {
        given().contentType("application/json")
                .body("{\"id\":\"ord-1\",\"product\":\"widget\",\"quantity\":5}")
                .post("/jms/send-pojo/pojo-queue")
                .then().statusCode(204);

        var order = get("/jms/receive-pojo/pojo-queue")
                .then().statusCode(200)
                .extract().as(new TypeRef<Map<String, Object>>() {
                });

        assertThat(order.get("id")).isEqualTo("ord-1");
        assertThat(order.get("product")).isEqualTo("widget");
        assertThat(order.get("quantity")).isEqualTo(5);
    }

    @Test
    void programmaticListenerReceivesMessages() {
        given().body("programmatic-msg")
                .post("/jms/send/programmatic-it-queue")
                .then().statusCode(204);

        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            List<String> received = get("/jms/programmatic/received")
                    .as(new TypeRef<>() {
                    });
            assertThat(received).contains("programmatic-msg");
        });
    }

    @Test
    void errorHandlerReceivesListenerErrors() {
        given().body("error-msg")
                .post("/jms/send/error-trigger-queue")
                .then().statusCode(204);

        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            List<String> errors = get("/jms/error-handler/errors")
                    .as(new TypeRef<>() {
                    });
            assertThat(errors).anySatisfy(e -> assertThat(e).contains("error-trigger-queue").contains("intentional error"));
        });
    }

    @Test
    void tracingSendCreatesProducerSpan() {
        delete("/jms/traces");

        given().body("traced-msg")
                .post("/jms/send/tracing-queue")
                .then().statusCode(204);

        await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
            List<Map<String, String>> spans = get("/jms/traces")
                    .as(new TypeRef<>() {
                    });
            assertThat(spans).anySatisfy(span -> {
                assertThat(span.get("name")).isEqualTo("tracing-queue publish");
                assertThat(span.get("kind")).isEqualTo("PRODUCER");
            });
        });
    }

    @Test
    void tracingListenerCreatesProcessSpanLinkedToProducer() {
        delete("/jms/traces");

        given().body("traced-listener-msg")
                .post("/jms/send/it-queue")
                .then().statusCode(204);

        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            List<String> received = get("/jms/listener/received")
                    .as(new TypeRef<>() {
                    });
            assertThat(received).contains("traced-listener-msg");
        });

        await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
            List<Map<String, String>> spans = get("/jms/traces")
                    .as(new TypeRef<>() {
                    });

            assertThat(spans).anySatisfy(span -> {
                assertThat(span.get("name")).isEqualTo("it-queue publish");
                assertThat(span.get("kind")).isEqualTo("PRODUCER");
            });
            assertThat(spans).anySatisfy(span -> {
                assertThat(span.get("name")).isEqualTo("it-queue process");
                assertThat(span.get("kind")).isEqualTo("CONSUMER");
            });

            // Verify trace propagation: producer and listener share the same trace ID
            String producerTraceId = spans.stream()
                    .filter(s -> "it-queue publish".equals(s.get("name")))
                    .map(s -> s.get("traceId"))
                    .findFirst().orElse(null);
            String consumerParentTraceId = spans.stream()
                    .filter(s -> "it-queue process".equals(s.get("name")))
                    .map(s -> s.get("parentTraceId"))
                    .findFirst().orElse(null);

            assertThat(producerTraceId).isNotNull();
            assertThat(consumerParentTraceId).isEqualTo(producerTraceId);
        });
    }
}
