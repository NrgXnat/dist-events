package io.xnatworks.events.distributed.components.hibernate;

import io.xnatworks.events.distributed.DistEventsPlugin;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.hibernate.Cache;
import org.hibernate.SessionFactory;
import org.nrg.xnat.services.XnatAppInfo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jms.annotation.JmsListener;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Component
@Slf4j
public class HibernateEntityUpdateTopicListener {
    private final Map<String, Class<?>> entityTypes = new ConcurrentHashMap<>();

    private final Cache  cache;
    private final String nodeId;

    @Autowired
    public HibernateEntityUpdateTopicListener(final SessionFactory sessionFactory, final XnatAppInfo appInfo) {
        cache  = sessionFactory.getCache();
        nodeId = appInfo.getNode().getNodeId();
    }

    @JmsListener(destination = DistEventsPlugin.DIST_EVENTS_TOPIC, selector = "messageClass = 'HibernateEntityUpdateMessage'", containerFactory = "jmsTopicListenerContainerFactory")
    public void receive(final HibernateEntityUpdateMessage message) {
        if (StringUtils.equals(nodeId, message.getOriginatingNodeId())) {
            log.info("Received message from this node so basically ignoring it: [{}] action '{}', entity type '{}', ID '{}'", message.getTimestamp(), message.getAction(), message.getEntityType(), message.getId());
            return;
        }
        log.info("Received message from node {}: [{}] action '{}', entity type '{}', ID '{}'", message.getOriginatingNodeId(), message.getTimestamp(), message.getAction(), message.getEntityType(), message.getId());
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
