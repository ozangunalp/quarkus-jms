package io.quarkiverse.jms.deployment.test;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.quarkiverse.jms.JmsListener;
import io.quarkus.test.QuarkusExtensionTest;

public class JmsListenerValidationTest {

    @RegisterExtension
    static final QuarkusExtensionTest config = new QuarkusExtensionTest()
            .overrideConfigKey("quarkus.http.test-port", "0")
            .withApplicationRoot(jar -> jar.addClasses(InvalidListener.class))
            .assertException(t -> {
                assertThat(t).hasMessageContaining("must have at least one parameter");
            });

    @Test
    void shouldFailBuild() {
    }

    public static class InvalidListener {

        @JmsListener("test-queue")
        public void onMessage() {
        }
    }
}
