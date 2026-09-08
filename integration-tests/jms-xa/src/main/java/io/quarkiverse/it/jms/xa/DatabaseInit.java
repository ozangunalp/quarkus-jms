package io.quarkiverse.it.jms.xa;

import javax.sql.DataSource;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;

import io.quarkus.runtime.StartupEvent;

@ApplicationScoped
public class DatabaseInit {

    @Inject
    DataSource dataSource;

    void onStart(@Observes StartupEvent ev) {
        try (var connection = dataSource.getConnection();
                var stmt = connection.createStatement()) {
            stmt.execute(
                    "CREATE TABLE IF NOT EXISTS xa_messages (id SERIAL PRIMARY KEY, payload VARCHAR(255))");
        } catch (Exception e) {
            throw new RuntimeException("Failed to create xa_messages table", e);
        }
    }
}
