package io.quarkiverse.it.jms;

import java.util.List;
import java.util.Map;

import io.quarkus.test.junit.QuarkusTestProfile;

public class FailoverTestProfile implements QuarkusTestProfile {

    @Override
    public Map<String, String> getConfigOverrides() {
        return Map.of("quarkus.artemis.devservices.enabled", "false");
    }

    @Override
    public List<TestResourceEntry> testResources() {
        return List.of(new TestResourceEntry(ArtemisTestResource.class));
    }
}
