package io.quarkiverse.spring.jms.runtime;

import io.quarkus.arc.Arc;
import io.quarkus.runtime.annotations.Recorder;

@Recorder
public class SpringJmsRecorder {

    public void configurePubSubDomain(boolean pubSubDomain) {
        if (pubSubDomain) {
            SpringJmsClientAdapter adapter = Arc.container().instance(SpringJmsClientAdapter.class).get();
            adapter.setPubSubDomain(true);
        }
    }
}
