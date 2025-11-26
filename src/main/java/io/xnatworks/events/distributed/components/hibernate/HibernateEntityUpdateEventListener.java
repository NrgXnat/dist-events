package io.xnatworks.events.distributed.components.hibernate;

import com.google.common.collect.ImmutableMap;
import io.xnatworks.events.distributed.components.DistEventsMessagePostProcessor;
import io.xnatworks.events.distributed.components.JmsTopicTemplate;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.hibernate.SessionFactory;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.envers.DefaultRevisionEntity;
import org.hibernate.event.service.spi.EventListenerRegistry;
import org.hibernate.event.spi.EventType;
import org.hibernate.event.spi.PostDeleteEvent;
import org.hibernate.event.spi.PostDeleteEventListener;
import org.hibernate.event.spi.PostInsertEvent;
import org.hibernate.event.spi.PostInsertEventListener;
import org.hibernate.event.spi.PostUpdateEvent;
import org.hibernate.event.spi.PostUpdateEventListener;
import org.hibernate.persister.entity.EntityPersister;
import org.nrg.dcm.scp.DicomSCPInstance;
import org.nrg.framework.orm.hibernate.BaseHibernateEntity;
import org.nrg.xft.utils.DateUtils;
import org.nrg.xnat.services.XnatAppInfo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.stereotype.Component;

import javax.jms.ConnectionFactory;
import javax.jms.Topic;
import javax.persistence.Entity;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static io.xnatworks.events.distributed.components.hibernate.Action.DELETE;
import static io.xnatworks.events.distributed.components.hibernate.Action.INSERT;
import static io.xnatworks.events.distributed.components.hibernate.Action.UPDATE;
import static io.xnatworks.events.distributed.components.hibernate.HibernateEntityUpdateMessage.AE_TITLE;
import static io.xnatworks.events.distributed.components.hibernate.HibernateEntityUpdateMessage.CRITICAL_ATTRIBUTES;
import static io.xnatworks.events.distributed.components.hibernate.HibernateEntityUpdateMessage.ENABLED;
import static io.xnatworks.events.distributed.components.hibernate.HibernateEntityUpdateMessage.PORT;

@Component
@Slf4j
public class HibernateEntityUpdateEventListener implements PostInsertEventListener, PostUpdateEventListener, PostDeleteEventListener {
    private static final long serialVersionUID = -7757627496623131826L;

    private static final DistEventsMessagePostProcessor POST_PROCESSOR         = new DistEventsMessagePostProcessor(HibernateEntityUpdateMessage.class);
    private static final List<Class<?>>                 IGNORED_ENTITY_CLASSES = Collections.singletonList(DefaultRevisionEntity.class);

    private final String      nodeId;
    private final JmsTemplate template;
    private final Topic       distEventsTopic;

    @Autowired
    public HibernateEntityUpdateEventListener(final XnatAppInfo appInfo, final ConnectionFactory connectionFactory, final SessionFactory sessionFactory, final Topic distEventsTopic) {
        this.nodeId          = appInfo.getNode().getNodeId();
        this.template        = new JmsTopicTemplate(connectionFactory);
        this.distEventsTopic = distEventsTopic;

        if (sessionFactory instanceof SessionFactoryImplementor) {
            final EventListenerRegistry registry = ((SessionFactoryImplementor) sessionFactory).getServiceRegistry().getService(EventListenerRegistry.class);
            registry.getEventListenerGroup(EventType.POST_INSERT).appendListener(this);
            registry.getEventListenerGroup(EventType.POST_UPDATE).appendListener(this);
            registry.getEventListenerGroup(EventType.POST_DELETE).appendListener(this);
        } else {
            log.warn("Could not register as Hibernate event listener: sessionFactory is not a SessionFactoryImplementor");
        }
    }

    @Override
    public void onPostInsert(final PostInsertEvent event) {
        final Object entity = event.getEntity();
        if (log.isDebugEnabled()) {
            log.debug("PostInsertEventListener onPostInsert got entity of type: {}", entity.getClass().getName());
        }
        if (entity instanceof DicomSCPInstance) {
            prepareDicomSCPInstanceInsert(event);
        } else {
            sendMessage(entity, INSERT);
        }
    }

    @Override
    public void onPostUpdate(final PostUpdateEvent event) {
        final Object entity = event.getEntity();
        if (log.isDebugEnabled()) {
            log.debug("PostUpdateEventListener onPostUpdate got entity of type: {}", entity.getClass().getName());
        }
        if (entity instanceof DicomSCPInstance) {
            prepareDicomSCPInstanceUpdate(event);
        } else {
            sendMessage(entity, UPDATE);
        }
    }

    @Override
    public void onPostDelete(final PostDeleteEvent event) {
        final Object entity = event.getEntity();
        if (log.isDebugEnabled()) {
            log.debug("PostDeleteEventListener onPostDelete got entity of type: {}", entity.getClass().getName());
        }
        if (entity instanceof DicomSCPInstance) {
            prepareDicomSCPInstanceDelete(event);
        } else {
            sendMessage(entity, DELETE);
        }
    }

    @Override
    public boolean requiresPostCommitHanding(final EntityPersister persister) {
        return false;
    }

    private void sendMessage(final Object entity, final Action action) {
        sendMessage(entity, action, Collections.emptyMap());
    }

    private void sendMessage(final Object entity, final Action action, final Map<String, String> properties) {
        if (!isEntityClass(entity)) {
            log.info("Got object that is not a Hibernate entity, not propagating: {}", entity.getClass().getName());
            return;
        }
        if (isIgnoredEntityClass(entity)) {
            log.info("Got Hibernate entity that is an ignored entity class, not propagating: {}", entity.getClass().getName());
            return;
        }
        final String entityType = entity.getClass().getName();
        final long   entityId   = getEntityId(entity);
        if (entityId == -1) {
            log.warn("Could not determine entity ID for {} entity of type {}, not sending event", action.getVerb(), entityType);
            return;
        }

        final boolean isDicomSCPInstance = entity instanceof DicomSCPInstance;
        if (action == INSERT && !isDicomSCPInstance) {
            log.debug("Not sending insert event for entity of type {} with ID {}: not a DicomSCPInstance", entity.getClass().getName(), entityId);
            return;
        }

        log.info("Entity of type {} {}: {}", entityType, action.getVerb(), entityId);
        final String timestamp = DateUtils.getMsTimestamp();
        template.convertAndSend(distEventsTopic, HibernateEntityUpdateMessage.builder()
                                                                             .originatingNodeId(nodeId)
                                                                             .timestamp(timestamp)
                                                                             .action(action)
                                                                             .entityType(entityType)
                                                                             .properties(properties)
                                                                             .id(entityId).build(), POST_PROCESSOR);
        log.debug("{}: sent message with timestamp '{}', action '{}', entity type '{}' for ID '{}'", nodeId, timestamp, action, entityType, entityId);
    }

    private void prepareDicomSCPInstanceInsert(final PostInsertEvent event) {
        sendMessage(event.getEntity(), INSERT, ImmutableMap.<String, String>builder()
                                                           .put(AE_TITLE, ((DicomSCPInstance) event.getEntity()).getAeTitle())
                                                           .put(ENABLED, getEnabledState(event.getEntity()))
                                                           .put(PORT, Integer.toString(((DicomSCPInstance) event.getEntity()).getPort()))
                                                           .build());
    }

    private void prepareDicomSCPInstanceUpdate(final PostUpdateEvent event) {
        final List<String> propertyNames   = Arrays.asList(event.getPersister().getPropertyNames());
        final int[]        dirtyProperties = event.getDirtyProperties();
        final List<String> changed = ArrayUtils.isNotEmpty(dirtyProperties)
                                     ? Arrays.stream(dirtyProperties)
                                             .mapToObj(propertyNames::get)
                                             .collect(Collectors.toList())
                                     : Collections.emptyList();

        if (CollectionUtils.isNotEmpty(changed)) {
            log.debug("DicomSCPInstance {} updated, changed properties: {}", getEntityId(event.getEntity()), String.join(", ", changed));
        } else {
            log.debug("DicomSCPInstance {} updated, but no changed properties detected", getEntityId(event.getEntity()));
        }

        // Update messages only require extended info if AE title or port was changed, because other nodes
        // will only be able to get the newly persisted values, not the previous values.
        if (CollectionUtils.containsAny(changed, CRITICAL_ATTRIBUTES)) {
            final ImmutableMap.Builder<String, String> properties = ImmutableMap.builder();
            if (changed.contains(AE_TITLE)) {
                properties.put(AE_TITLE, (String) event.getOldState()[propertyNames.indexOf(AE_TITLE)]);
            }
            if (changed.contains(ENABLED)) {
                properties.put(ENABLED, getEnabledState(event.getEntity()));
            }
            if (changed.contains(PORT)) {
                properties.put(PORT, Integer.toString((int) event.getOldState()[propertyNames.indexOf(PORT)]));
            }
            sendMessage(event.getEntity(), UPDATE, properties.build());
        } else {
            sendMessage(event.getEntity(), UPDATE);
        }
    }

    private void prepareDicomSCPInstanceDelete(final PostDeleteEvent event) {
        sendMessage(event.getEntity(), DELETE, ImmutableMap.<String, String>builder()
                                                           .put(AE_TITLE, ((DicomSCPInstance) event.getEntity()).getAeTitle())
                                                           .put(PORT, String.valueOf(((DicomSCPInstance) event.getEntity()).getPort()))
                                                           .build());

    }

    private static boolean isEntityClass(final Object entity) {
        return BaseHibernateEntity.class.isAssignableFrom(entity.getClass()) || entity.getClass().isAnnotationPresent(Entity.class);
    }

    private static boolean isIgnoredEntityClass(final Object entity) {
        return IGNORED_ENTITY_CLASSES.stream().anyMatch(ignoredClass -> ignoredClass.isAssignableFrom(entity.getClass()));
    }

    private static long getEntityId(final Object entity) {
        if (entity instanceof BaseHibernateEntity) {
            return ((BaseHibernateEntity) entity).getId();
        }
        try {
            return (long) entity.getClass().getMethod("getId").invoke(entity);
        } catch (Exception e) {
            log.error("Could not retrieve entity ID on an instance of type {}", entity.getClass().getName(), e);
            return -1;
        }
    }

    private static String getEnabledState(final Object instance) {
        return ((DicomSCPInstance) instance).isEnabled() ? "enabled" : "disabled";
    }
}
