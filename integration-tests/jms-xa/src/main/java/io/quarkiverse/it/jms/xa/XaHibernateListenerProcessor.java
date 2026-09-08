package io.quarkiverse.it.jms.xa;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.persistence.EntityManager;

import org.jboss.logging.Logger;

import io.quarkiverse.jms.JmsListener;

@ApplicationScoped
public class XaHibernateListenerProcessor {

    private static final Logger LOG = Logger.getLogger(XaHibernateListenerProcessor.class);

    @Inject
    EntityManager entityManager;

    @JmsListener(value = "xa-hibernate-queue", acknowledge = JmsListener.AcknowledgeMode.XA)
    public void process(String payload) {
        LOG.infof("Processing XA Hibernate message: %s", payload);
        entityManager.persist(new XaMessageEntity("hibernate:" + payload));
    }

    @JmsListener(value = "xa-hibernate-rollback-queue", acknowledge = JmsListener.AcknowledgeMode.XA)
    public void processAndFail(String payload) {
        LOG.infof("Processing XA Hibernate message (will fail): %s", payload);
        entityManager.persist(new XaMessageEntity("should-not-persist-hibernate:" + payload));
        throw new RuntimeException("Intentional failure to trigger XA rollback");
    }
}
