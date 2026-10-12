package io.xnatworks.events.distributed.components.hibernate;

import org.hibernate.event.spi.AbstractEvent;
import org.hibernate.event.spi.PostUpdateEvent;
import org.nrg.xnat.hibernate.listeners.methods.AbstractHibernateEntityEventHandlerMethod;
import org.nrg.xnat.hibernate.listeners.methods.HibernateEntityEventHandlerMethod;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Future;

/**
 * Handles updates to {@link LargeObjectEntity} by recording the entity each event carries.
 */
public class RecordingUpdateHandlerMethod extends AbstractHibernateEntityEventHandlerMethod {
    private final List<LargeObjectEntity> entities = new CopyOnWriteArrayList<>();

    public RecordingUpdateHandlerMethod() {
        super(HibernateEntityEventHandlerMethod.Action.UPDATE, LargeObjectEntity.class);
    }

    @Override
    public Future<Boolean> handleEvent(final AbstractEvent event) {
        entities.add((LargeObjectEntity) ((PostUpdateEvent) event).getEntity());
        return CompletableFuture.completedFuture(true);
    }

    public List<LargeObjectEntity> getEntities() {
        return entities;
    }
}
