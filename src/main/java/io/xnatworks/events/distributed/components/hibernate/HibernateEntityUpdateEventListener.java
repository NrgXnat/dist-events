package io.xnatworks.events.distributed.components.hibernate;

import com.google.common.collect.ImmutableMap;
import io.xnatworks.events.distributed.components.DistEventsMessagePostProcessor;
import io.xnatworks.events.distributed.components.JmsTopicTemplate;
import lombok.extern.slf4j.Slf4j;
import org.hibernate.SessionFactory;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.event.service.spi.EventListenerRegistry;
import org.hibernate.event.spi.EventType;
import org.hibernate.event.spi.PostDeleteEvent;
import org.hibernate.event.spi.PostDeleteEventListener;
import org.hibernate.event.spi.PostUpdateEvent;
import org.hibernate.event.spi.PostUpdateEventListener;
import org.hibernate.persister.entity.EntityPersister;
import org.nrg.framework.orm.hibernate.BaseHibernateEntity;
import org.nrg.xft.utils.DateUtils;
import org.nrg.xnat.services.XnatAppInfo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.stereotype.Component;

import javax.jms.ConnectionFactory;
import javax.jms.Topic;
import java.util.Map;

@Component
@Slf4j
public class HibernateEntityUpdateEventListener implements PostUpdateEventListener, PostDeleteEventListener {
    private static final long serialVersionUID = -7757627496623131826L;

    private static final String                         UPDATE         = EventType.POST_UPDATE.eventName();
    private static final String                         DELETE         = EventType.POST_DELETE.eventName();
    private static final Map<String, String>            ACTIONS        = ImmutableMap.<String, String>builder().put(UPDATE, "updated").put(DELETE, "deleted").build();
    private static final DistEventsMessagePostProcessor POST_PROCESSOR = new DistEventsMessagePostProcessor(HibernateEntityUpdateMessage.class);

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
            registry.getEventListenerGroup(EventType.POST_UPDATE).appendListener(this);
            registry.getEventListenerGroup(EventType.POST_DELETE).appendListener(this);
        } else {
            log.warn("Could not register as Hibernate event listener: sessionFactory is not a SessionFactoryImplementor");
        }
    }

    @Override
    public void onPostUpdate(final PostUpdateEvent event) {
        sendMessage(event.getEntity(), UPDATE);
    }

    @Override
    public void onPostDelete(final PostDeleteEvent event) {
        sendMessage(event.getEntity(), DELETE);
    }

    @Override
    public boolean requiresPostCommitHanding(final EntityPersister persister) {
        return false;
    }

    private void sendMessage(final Object entity, final String action) {
        final String entityType = entity.getClass().getName();
        final long   entityId   = getEntityId(entity);
        final String verb       = ACTIONS.get(action);
        if (entityId == -1) {
            log.warn("Could not determine entity ID for {} entity of type {}, not sending event", verb, entityType);
            return;
        }

        log.info("Entity of type {} {}: {}", entityType, verb, entityId);
        final String timestamp = DateUtils.getMsTimestamp();
        template.convertAndSend(distEventsTopic,
                                HibernateEntityUpdateMessage.builder()
                                                            .originatingNodeId(nodeId)
                                                            .timestamp(timestamp)
                                                            .action(action)
                                                            .entityType(entityType)
                                                            .id(entityId)
                                                            .build(),
                                POST_PROCESSOR);
        log.debug("{}: sent message with timestamp '{}', action '{}', entity type '{}' for ID '{}'", nodeId, timestamp, action, entityType, entityId);
    }

    private static long getEntityId(final Object entity) {
        if (entity instanceof BaseHibernateEntity) {
            return ((BaseHibernateEntity) entity).getId();
        }
        try {
            return (long) entity.getClass().getMethod("getId").invoke(entity);
        } catch (Exception e) {
            log.error("Could not retrieve entity ID", e);
            return -1;
        }
    }
}
