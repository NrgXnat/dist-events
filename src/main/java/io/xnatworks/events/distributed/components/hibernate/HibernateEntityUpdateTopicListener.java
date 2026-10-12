package io.xnatworks.events.distributed.components.hibernate;

import io.xnatworks.events.distributed.DistEventsPlugin;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.hibernate.Cache;
import org.hibernate.SessionFactory;
import org.hibernate.cfg.AvailableSettings;
import org.hibernate.event.spi.AbstractEvent;
import org.nrg.dcm.scp.DicomSCPEvent;
import org.nrg.framework.services.NrgEventServiceI;
import org.nrg.xnat.hibernate.listeners.methods.HibernateEntityEventHandlerMethod;
import org.nrg.xnat.services.XnatAppInfo;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jms.annotation.JmsListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import javax.persistence.CacheRetrieveMode;
import javax.persistence.CacheStoreMode;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static io.xnatworks.events.distributed.components.hibernate.HibernateEntityUpdateMessage.AE_TITLE;
import static io.xnatworks.events.distributed.components.hibernate.HibernateEntityUpdateMessage.PORT;

@Component
@Slf4j
public class HibernateEntityUpdateTopicListener {
    private static final String              DICOM_SCP_INSTANCE_CLASS_NAME = "org.nrg.dcm.scp.DicomSCPInstance";
    private static final Map<String, Object> BYPASS_CACHE                  = Map.of(AvailableSettings.JPA_SHARED_CACHE_RETRIEVE_MODE, CacheRetrieveMode.BYPASS,
                                                                                    AvailableSettings.JPA_SHARED_CACHE_STORE_MODE, CacheStoreMode.BYPASS);

    private final Map<String, Optional<Class<?>>> entityTypes = new ConcurrentHashMap<>();

    private final NrgEventServiceI                        eventService;
    private final SessionFactory                          sessionFactory;
    private final TransactionTemplate                     readOnlyTransaction;
    private final Cache                                   cache;
    private final String                                  nodeId;
    private final List<HibernateEntityEventHandlerMethod> handlerMethods;

    @Autowired
    public HibernateEntityUpdateTopicListener(final NrgEventServiceI eventService, final SessionFactory sessionFactory, final PlatformTransactionManager transactionManager, final XnatAppInfo appInfo, final List<HibernateEntityEventHandlerMethod> handlerMethods) {
        this(eventService, sessionFactory, transactionManager, appInfo.getNode().getNodeId(), handlerMethods);
    }

    HibernateEntityUpdateTopicListener(final NrgEventServiceI eventService, final SessionFactory sessionFactory, final PlatformTransactionManager transactionManager, final String nodeId, final List<HibernateEntityEventHandlerMethod> handlerMethods) {
        this.eventService        = eventService;
        this.sessionFactory      = sessionFactory;
        this.readOnlyTransaction = new TransactionTemplate(transactionManager);
        this.readOnlyTransaction.setReadOnly(true);
        this.cache               = sessionFactory.getCache();
        this.nodeId              = nodeId;
        this.handlerMethods      = handlerMethods;
    }

    @JmsListener(destination = DistEventsPlugin.DIST_EVENTS_TOPIC, selector = "messageClass = 'HibernateEntityUpdateMessage'", containerFactory = "jmsTopicListenerContainerFactory")
    public void receive(final HibernateEntityUpdateMessage message) {
        if (StringUtils.equals(nodeId, message.getOriginatingNodeId())) {
            log.info("Received message from this node so basically ignoring it: [{}] action '{}', entity type '{}', ID '{}'", message.getTimestamp(), message.getAction(), message.getEntityType(), message.getId());
            return;
        }
        log.info("Received message from node {}: [{}] action '{}', entity type '{}', ID '{}'", message.getOriginatingNodeId(), message.getTimestamp(), message.getAction(), message.getEntityType(), message.getId());

        final Class<?> entityType = getEntityType(message.getEntityType());
        if (entityType == null) {
            return;
        }

        // Evict before anything can fail, and so a handler gets the entity from the database rather than this
        // node's cached copy. We don't need to clear the cache for inserts.
        if (message.getAction() != Action.INSERT) {
            log.debug("Evicting cache for entity type {} and ID {}", entityType, message.getId());
            cache.evictEntityData(entityType, message.getId());
        }

        // A failure here mustn't stop the DICOM SCP propagation below
        try {
            handleEvent(message, entityType);
        } catch (Exception e) {
            log.error("Error handling event for message: [{}] action '{}', entity type '{}', ID '{}'", message.getTimestamp(), message.getAction(), message.getEntityType(), message.getId(), e);
        }

        if (StringUtils.equals(DICOM_SCP_INSTANCE_CLASS_NAME, message.getEntityType())) {
            final DicomSCPEvent.DicomSCPEventBuilder<?, ?> builder = DicomSCPEvent.builder();

            final Action action = message.getAction();
            builder.action(action.toString());

            final long id = message.getId().longValue();
            switch (action) {
                case INSERT:
                    builder.id(id);
                    break;
                case UPDATE:
                    builder.id(id);
                    final String aeTitle = message.getProperties().get(AE_TITLE);
                    if (StringUtils.isNotBlank(aeTitle)) {
                        builder.aeTitle(aeTitle);
                    }
                    final String port = message.getProperties().get(PORT);
                    if (StringUtils.isNotBlank(port)) {
                        builder.port(Integer.parseInt(port));
                    }
                    break;
                case DELETE:
                    builder.id(id)
                           .aeTitle(message.getProperties().get(AE_TITLE))
                           .port(Integer.parseInt(message.getProperties().get(PORT)));
                    break;
                default:
                    log.warn("Received message with unrecognized action '{}', ignoring.", action);
                    return;
            }

            final DicomSCPEvent event = builder.build();
            log.info("Triggering DicomSCPEvent: {}", event);
            eventService.triggerEvent(event);
        }
    }

    /**
     * Calls the handler methods that match the message. The entity is only loaded once a handler method matches the
     * event with the class name standing in for the entity, as deletes already do, so most messages never touch the
     * database and handler methods get a detached entity.
     */
    private void handleEvent(final HibernateEntityUpdateMessage message, final Class<?> entityType) {
        final AbstractEvent                           placeholder = message.toHibernateEvent(entityType, entityType.getName());
        final List<HibernateEntityEventHandlerMethod> candidates  = handlerMethods.stream().filter(method -> method.matches(placeholder)).collect(Collectors.toList());
        if (candidates.isEmpty()) {
            log.debug("No handler methods for {} of entity type {}, not loading entity {}", message.getAction(), entityType.getName(), message.getId());
            return;
        }

        // Deletes carry their placeholder, so only inserts and updates need the entity
        final AbstractEvent event;
        if (message.getAction() == Action.DELETE) {
            event = placeholder;
        } else {
            final Object entity = loadEntity(message, entityType);
            if (entity == null) {
                return;
            }
            event = message.toHibernateEvent(entityType, entity);
        }

        candidates.stream().filter(method -> {
            final boolean matches = method.matches(event);
            if (log.isDebugEnabled()) {
                log.debug("Found a handler method of type {} that {}", AopUtils.getTargetClass(method), matches ? "matches" : "doesn't match");
            }
            return matches;
        }).forEach(method -> {
            try {
                if (log.isDebugEnabled()) {
                    log.debug("Found a matching handler method of type {}, calling handleEvent()", method.getClass().getName());
                }
                method.handleEvent(event);
            } catch (Exception e) {
                log.error("Error handling event for message: [{}] action '{}', entity type '{}', ID '{}'", message.getTimestamp(), message.getAction(), message.getEntityType(), message.getId(), e);
            }
        });
    }

    /**
     * Loads the entity in its own read-only transaction, so its session, database connection and the entity itself
     * are released before the next message. The transaction also lets PostgreSQL read large-object columns, which
     * it refuses in auto-commit mode. The load bypasses the second-level cache: the other node sends this message
     * before its transaction commits, so caching what this reads could keep the entity as it was before the change.
     *
     * @return The entity, or null if it no longer exists
     */
    private Object loadEntity(final HibernateEntityUpdateMessage message, final Class<?> entityType) {
        final Object entity = readOnlyTransaction.execute(status -> sessionFactory.getCurrentSession().find(entityType, message.getId(), BYPASS_CACHE));
        if (entity == null) {
            log.warn("Entity of type {} with ID {} not found, not calling handler methods for message [{}] action '{}'", entityType.getName(), message.getId(), message.getTimestamp(), message.getAction());
        }
        return entity;
    }

    /**
     * Resolves the entity class, remembering types this node can't load (say, from a plugin installed only on the
     * other node) so each one is logged once rather than on every message.
     */
    private Class<?> getEntityType(final String entityType) {
        return entityTypes.computeIfAbsent(entityType, key -> {
            try {
                return Optional.of(Class.forName(key));
            } catch (ClassNotFoundException e) {
                log.error("Could not find class for entity type {}, skipping cache eviction and handler methods for messages about it.", key);
                return Optional.empty();
            }
        }).orElse(null);
    }
}
