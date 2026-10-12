package io.xnatworks.events.distributed.components.hibernate;

import org.hibernate.event.spi.AbstractEvent;
import org.hibernate.event.spi.PostDeleteEvent;
import org.hibernate.event.spi.PostInsertEvent;
import org.hibernate.event.spi.PostUpdateEvent;
import org.nrg.xnat.hibernate.listeners.methods.AbstractHibernateEntityEventHandlerMethod;
import org.nrg.xnat.hibernate.listeners.methods.HibernateEntityEventHandlerMethod;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Future;

/**
 * Handles one action on {@link LargeObjectEntity} by recording the entity each event carries.
 */
public class RecordingHandlerMethod extends AbstractHibernateEntityEventHandlerMethod {
    private final List<Object> entities = new CopyOnWriteArrayList<>();

    public RecordingHandlerMethod(final HibernateEntityEventHandlerMethod.Action action) {
        super(action, LargeObjectEntity.class);
    }

    @Override
    public Future<Boolean> handleEvent(final AbstractEvent event) {
        entities.add(switch (event) {
            case final PostInsertEvent insert -> insert.getEntity();
            case final PostUpdateEvent update -> update.getEntity();
            case final PostDeleteEvent delete -> delete.getEntity();
            default -> throw new IllegalArgumentException("Unexpected event " + event.getClass().getName());
        });
        return CompletableFuture.completedFuture(true);
    }

    public List<Object> getEntities() {
        return entities;
    }

    public LargeObjectEntity getLastEntity() {
        return (LargeObjectEntity) entities.getLast();
    }
}
