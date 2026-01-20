package io.xnatworks.events.distributed.components.prefs;

import io.xnatworks.events.distributed.DistEventsPlugin;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.hibernate.Cache;
import org.hibernate.SessionFactory;
import org.nrg.framework.services.NrgEventServiceI;
import org.nrg.prefs.beans.PreferenceBean;
import org.nrg.prefs.entities.Preference;
import org.nrg.prefs.exceptions.InvalidPreferenceName;
import org.nrg.xdat.preferences.PreferenceEvent;
import org.nrg.xnat.services.XnatAppInfo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jms.annotation.JmsListener;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

@Component
@Slf4j
public class XnatPreferenceUpdateTopicListener {
    private final NrgEventServiceI                      eventService;
    private final Cache                                 cache;
    private final Map<String, ? extends PreferenceBean> preferenceBeans;
    private final XnatPreferenceUpdateHandlerMethod     handlerMethod;
    private final String                                nodeId;

    @Autowired
    public XnatPreferenceUpdateTopicListener(final NrgEventServiceI eventService, final SessionFactory sessionFactory, final List<? extends PreferenceBean> preferenceBeans, final XnatPreferenceUpdateHandlerMethod handlerMethod, final XnatAppInfo appInfo) {
        this.eventService    = eventService;
        this.cache           = sessionFactory.getCache();
        this.preferenceBeans = preferenceBeans.stream().collect(Collectors.toMap(PreferenceBean::getToolId, Function.identity()));
        this.handlerMethod   = handlerMethod;
        this.nodeId          = appInfo.getNode().getNodeId();
    }

    @JmsListener(destination = DistEventsPlugin.DIST_EVENTS_TOPIC, selector = "messageClass = 'XnatPreferenceUpdateMessage'", containerFactory = "jmsTopicListenerContainerFactory")
    public void receive(final XnatPreferenceUpdateMessage message) {
        final String originatingNodeId = message.getOriginatingNodeId();
        final String timestamp         = message.getTimestamp();
        final String toolId            = message.getToolId();
        final String preference        = message.getPreference();
        final String value             = message.getValue();
        if (StringUtils.equals(nodeId, originatingNodeId)) {
            log.info("Received message from this node so basically ignoring it: [{}] tool ID '{}', preference '{}', value '{}'", timestamp, toolId, preference, value);
            return;
        }
        if (!preferenceBeans.containsKey(toolId)) {
            log.warn("Received message from node {} for an update to tool ID {} preference {}, but I don't have that tool ID.", originatingNodeId, toolId, preference);
            return;
        }
        if (handlerMethod.isExistingPreferenceValue(toolId, preference, value)) {
            log.info("Received message from node {} for an update to tool ID {} preference {}, but that preference is already set to value: {}", originatingNodeId, toolId, preference, value);
            return;
        }
        final PreferenceBean preferenceBean = preferenceBeans.get(toolId);
        if (!preferenceBean.containsKey(preference)) {
            log.warn("Received message from node {} for an update to tool ID {} preference {}, but that tool does not have that preference.", originatingNodeId, toolId, preference);
            return;
        }
        try {
            log.info("Received message from node {} for an update to tool ID {} preference {} to value: {}", originatingNodeId, toolId, preference, value);
            handlerMethod.addExistingPreferenceValue(toolId, preference, value);
            preferenceBean.invalidate(preference);
            clearEntityCache(preferenceBean.getPreference(preference).getId());
            eventService.triggerEvent(new PreferenceEvent(preference, value));
        } catch (InvalidPreferenceName e) {
            throw new RuntimeException(e);
        }
    }

    private void clearEntityCache(final long preferenceId) {
        log.debug("Evicting cache for preference ID {}", preferenceId);
        cache.evictEntityData(Preference.class, preferenceId);
    }
}
