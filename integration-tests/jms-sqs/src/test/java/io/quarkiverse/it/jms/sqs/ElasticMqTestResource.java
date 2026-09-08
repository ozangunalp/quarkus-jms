package io.quarkiverse.it.jms.sqs;

import java.net.URI;
import java.util.Map;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;

public class ElasticMqTestResource implements QuarkusTestResourceLifecycleManager {

    private GenericContainer<?> elasticMq;
    private SqsClient sqsClient;

    @Override
    public Map<String, String> start() {
        elasticMq = new GenericContainer<>(
                DockerImageName.parse("softwaremill/elasticmq-native:latest"))
                .withExposedPorts(9324)
                .waitingFor(Wait.forListeningPort());
        elasticMq.start();

        String endpoint = "http://" + elasticMq.getHost() + ":" + elasticMq.getMappedPort(9324);

        // Create queues using the SQS client
        sqsClient = SqsClient.builder()
                .endpointOverride(URI.create(endpoint))
                .region(Region.US_EAST_1)
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create("test", "test")))
                .build();
        sqsClient.createQueue(r -> r.queueName("test-queue"));
        sqsClient.createQueue(r -> r.queueName("listener-queue"));

        return Map.of("sqs.endpoint", endpoint);
    }

    @Override
    public void stop() {
        if (sqsClient != null) {
            sqsClient.close();
        }
        if (elasticMq != null) {
            elasticMq.stop();
        }
    }
}
