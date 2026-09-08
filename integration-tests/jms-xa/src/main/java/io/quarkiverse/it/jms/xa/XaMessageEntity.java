package io.quarkiverse.it.jms.xa;

import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

@Entity
@Table(name = "xa_jpa_messages")
public class XaMessageEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    public Long id;

    public String payload;

    public XaMessageEntity() {
    }

    public XaMessageEntity(String payload) {
        this.payload = payload;
    }
}
