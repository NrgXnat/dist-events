package io.xnatworks.events.distributed.components.hibernate;

import lombok.Getter;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.Cache;
import org.hibernate.annotations.CacheConcurrencyStrategy;

import javax.persistence.Cacheable;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Lob;

/**
 * A cached entity with a large-object column, like XNAT's SubscriptionDeliveryEntity. On PostgreSQL a {@link Lob}
 * string is stored as a large object, which the JDBC driver only reads inside a transaction.
 */
@Entity
@Cacheable
@Cache(usage = CacheConcurrencyStrategy.READ_WRITE)
@Getter
@NoArgsConstructor
public class LargeObjectEntity {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Integer id;

    @Lob
    private String payload;

    public LargeObjectEntity(final String payload) {
        this.payload = payload;
    }
}
