package io.xnatworks.events.distributed.components.prefs;

import io.xnatworks.events.distributed.DistEventsPlugin;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.nrg.prefs.beans.PreferenceBean;
import org.nrg.prefs.exceptions.InvalidPreferenceName;
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
    private final Map<String, ? extends PreferenceBean> preferenceBeans;
    private final String                                nodeId;

    @Autowired
    public XnatPreferenceUpdateTopicListener(final List<? extends PreferenceBean> preferenceBeans, final XnatAppInfo appInfo) {
        this.preferenceBeans = preferenceBeans.stream().collect(Collectors.toMap(PreferenceBean::getToolId, Function.identity()));
        this.nodeId          = appInfo.getNode().getNodeId();
    }

    @JmsListener(destination = DistEventsPlugin.DIST_EVENTS_TOPIC, selector = "messageClass = 'XnatPreferenceUpdateMessage'", containerFactory = "jmsTopicListenerContainerFactory")
    public void receive(final XnatPreferenceUpdateMessage message) {
        if (StringUtils.equals(nodeId, message.getOriginatingNodeId())) {
            log.info("Received message from this node so basically ignoring it: [{}] tool ID '{}', preference '{}', value '{}'", message.getTimestamp(), message.getToolId(), message.getPreference(), message.getValue());
            return;
        }
        if (!preferenceBeans.containsKey(message.getToolId())) {
            log.warn("Received message from node {} for an update to tool ID {} preference {}, but I don't have that tool ID.", message.getOriginatingNodeId(), message.getToolId(), message.getPreference());
            return;
        }
        final PreferenceBean preferenceBean = preferenceBeans.get(message.getToolId());
        if (!preferenceBean.containsKey(message.getPreference())) {
            log.warn("Received message from node {} for an update to tool ID {} preference {}, but that tool does not have that preference.", message.getOriginatingNodeId(), message.getToolId(), message.getPreference());
            return;
        }
        try {
            preferenceBean.invalidate(message.getPreference());
        } catch (InvalidPreferenceName e) {
            throw new RuntimeException(e);
        }
    }
}
