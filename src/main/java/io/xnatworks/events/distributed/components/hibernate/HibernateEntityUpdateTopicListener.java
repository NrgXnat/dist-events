package io.xnatworks.events.distributed.components.hibernate;

import io.xnatworks.events.distributed.DistEventsPlugin;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.hibernate.Cache;
import org.hibernate.SessionFactory;
import org.hibernate.event.spi.AbstractEvent;
import org.nrg.dcm.scp.DicomSCPEvent;
import org.nrg.framework.services.NrgEventServiceI;
import org.nrg.xnat.hibernate.listeners.methods.HibernateEntityEventHandlerMethod;
import org.nrg.xnat.services.XnatAppInfo;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jms.annotation.JmsListener;
import org.springframework.stereotype.Component;

import javax.persistence.EntityManager;
import java.lang.reflect.Proxy;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static io.xnatworks.events.distributed.components.hibernate.HibernateEntityUpdateMessage.AE_TITLE;
import static io.xnatworks.events.distributed.components.hibernate.HibernateEntityUpdateMessage.PORT;

@Component
@Slf4j
public class HibernateEntityUpdateTopicListener {
    private static final String DICOM_SCP_INSTANCE_CLASS_NAME = "org.nrg.dcm.scp.DicomSCPInstance";

    private final Map<String, Class<?>> entityTypes = new ConcurrentHashMap<>();

    private final NrgEventServiceI                        eventService;
    private final EntityManager                           entityManager;
    private final Cache                                   cache;
    private final String                                  nodeId;
    private final List<HibernateEntityEventHandlerMethod> handlerMethods;

    @Autowired
    public HibernateEntityUpdateTopicListener(final NrgEventServiceI eventService, final EntityManager entityManager, final SessionFactory sessionFactory, final XnatAppInfo appInfo, final List<HibernateEntityEventHandlerMethod> handlerMethods) {
        this.eventService   = eventService;
        this.entityManager  = entityManager;
        this.cache          = sessionFactory.getCache();
        this.nodeId         = appInfo.getNode().getNodeId();
        this.handlerMethods = handlerMethods;
    }

    @JmsListener(destination = DistEventsPlugin.DIST_EVENTS_TOPIC, selector = "messageClass = 'HibernateEntityUpdateMessage'", containerFactory = "jmsTopicListenerContainerFactory")
    public void receive(final HibernateEntityUpdateMessage message) {
        if (StringUtils.equals(nodeId, message.getOriginatingNodeId())) {
            log.info("Received message from this node so basically ignoring it: [{}] action '{}', entity type '{}', ID '{}'", message.getTimestamp(), message.getAction(), message.getEntityType(), message.getId());
            return;
        }
        log.info("Received message from node {}: [{}] action '{}', entity type '{}', ID '{}'", message.getOriginatingNodeId(), message.getTimestamp(), message.getAction(), message.getEntityType(), message.getId());

        try {
            final AbstractEvent event = message.toHibernateEvent(entityManager);

            // We don't need to clear the cache for inserts
            if (message.getAction() != Action.INSERT) {
                clearEntityCache(message);
            }

            handlerMethods.stream().filter(method -> {
                final boolean matches = method.matches(event);
                if (log.isDebugEnabled()) {
                    final Class<?> methodClass;
                    if (Proxy.isProxyClass(method.getClass())) {
                        methodClass = AopUtils.getTargetClass(method);
                    } else {
                        methodClass = method.getClass();
                    }
                    log.debug("Found a handler method of type {} that {}", methodClass, matches ? "matches" : "doesn't match");
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
        } catch (ClassNotFoundException e) {
            log.error("Could not find class for entity type {}, skipping cache eviction and handler methods for this message.", message.getEntityType(), e);
            return;
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

    private void clearEntityCache(final HibernateEntityUpdateMessage message) {
        final Class<?> entityType = entityTypes.computeIfAbsent(message.getEntityType(), key -> {
            try {
                return Class.forName(key);
            } catch (ClassNotFoundException e) {
                log.error("Could not find class for entity type {}", key, e);
                return null;
            }
        });
        log.debug("Evicting cache for entity type {} and ID {}", entityType, message.getId());
        cache.evictEntityData(entityType, message.getId());
    }
}
