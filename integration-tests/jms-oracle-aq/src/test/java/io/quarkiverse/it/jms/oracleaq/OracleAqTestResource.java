package io.quarkiverse.it.jms.oracleaq;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Map;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;

public class OracleAqTestResource implements QuarkusTestResourceLifecycleManager {

    private GenericContainer<?> oracle;

    @Override
    public Map<String, String> start() {
        oracle = new GenericContainer<>(
                DockerImageName.parse("gvenzl/oracle-free:23-slim-faststart"))
                .withExposedPorts(1521)
                .withEnv("ORACLE_PASSWORD", "testpassword")
                .waitingFor(Wait.forLogMessage(".*DATABASE IS READY TO USE.*", 1))
                .withStartupTimeout(java.time.Duration.ofMinutes(5));
        oracle.start();

        String jdbcUrl = "jdbc:oracle:thin:@" + oracle.getHost() + ":" + oracle.getMappedPort(1521) + "/FREEPDB1";
        String user = "SYSTEM";
        String password = "testpassword";

        createQueues(jdbcUrl, user, password);

        return Map.of(
                "oracle.jdbc.url", jdbcUrl,
                "oracle.jdbc.user", user,
                "oracle.jdbc.password", password);
    }

    private void createQueues(String jdbcUrl, String user, String password) {
        try (Connection connection = DriverManager.getConnection(jdbcUrl, user, password);
                Statement stmt = connection.createStatement()) {
            stmt.execute("BEGIN " +
                    "DBMS_AQADM.CREATE_QUEUE_TABLE(queue_table => 'TEST_QT', " +
                    "queue_payload_type => 'SYS.AQ$_JMS_TEXT_MESSAGE'); " +
                    "DBMS_AQADM.CREATE_QUEUE(queue_name => 'TEST_QUEUE', queue_table => 'TEST_QT'); " +
                    "DBMS_AQADM.START_QUEUE(queue_name => 'TEST_QUEUE'); " +
                    "DBMS_AQADM.CREATE_QUEUE_TABLE(queue_table => 'LISTENER_QT', " +
                    "queue_payload_type => 'SYS.AQ$_JMS_TEXT_MESSAGE'); " +
                    "DBMS_AQADM.CREATE_QUEUE(queue_name => 'LISTENER_QUEUE', queue_table => 'LISTENER_QT'); " +
                    "DBMS_AQADM.START_QUEUE(queue_name => 'LISTENER_QUEUE'); " +
                    "END;");
        } catch (Exception e) {
            throw new RuntimeException("Failed to create Oracle AQ queues", e);
        }
    }

    @Override
    public void stop() {
        if (oracle != null) {
            oracle.stop();
        }
    }
}
