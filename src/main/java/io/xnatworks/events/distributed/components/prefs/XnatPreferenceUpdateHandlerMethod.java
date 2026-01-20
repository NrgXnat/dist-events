package io.xnatworks.events.distributed.components.prefs;

import io.xnatworks.events.distributed.components.DistEventsMessagePostProcessor;
import io.xnatworks.events.distributed.components.JmsTopicTemplate;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.nrg.prefs.beans.AbstractPreferenceBean;
import org.nrg.xdat.preferences.EventTriggeringAbstractPreferenceBean;
import org.nrg.xft.utils.DateUtils;
import org.nrg.xnat.event.listeners.methods.AbstractXnatPreferenceHandlerMethod;
import org.nrg.xnat.services.XnatAppInfo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.stereotype.Component;

import javax.jms.ConnectionFactory;
import javax.jms.Topic;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

@Component
@Slf4j
public class XnatPreferenceUpdateHandlerMethod extends AbstractXnatPreferenceHandlerMethod {
    private static final DistEventsMessagePostProcessor POST_PROCESSOR = new DistEventsMessagePostProcessor(XnatPreferenceUpdateMessage.class);

    private final Map<String, String> existingPreferences = new ConcurrentHashMap<>();

    private final Map<String, Set<String>> preferences;
    private final String                   nodeId;
    private final JmsTemplate              template;
    private final Topic                    distEventsTopic;

    @Autowired
    public XnatPreferenceUpdateHandlerMethod(final List<? extends EventTriggeringAbstractPreferenceBean> preferenceBeans, final XnatAppInfo appInfo, final ConnectionFactory connectionFactory, final Topic distEventsTopic) {
        // This hinky thing here makes this method work with all event-triggering preference beans in the application context, supporting all of their preferences.
        super(preferenceBeans.stream().map(EventTriggeringAbstractPreferenceBean::getClass).collect(Collectors.toList()),
              preferenceBeans.stream().flatMap(bean -> bean.keySet().stream()).toArray(String[]::new));
        final Map<String, Set<String>> preferenceMap = preferenceBeans.stream().collect(Collectors.toMap(AbstractPreferenceBean::getToolId, AbstractPreferenceBean::keySet));
        this.preferences     = invertPreferenceMap(preferenceMap);
        this.nodeId          = appInfo.getNode().getNodeId();
        this.template        = new JmsTopicTemplate(connectionFactory);
        this.distEventsTopic = distEventsTopic;
    }

    @Override
    protected void handlePreferenceImpl(final String preference, final String value) {
        if (!preferences.containsKey(preference)) {
            log.warn("No tools registered for preference {}, not sending update message", preference);
            return;
        }
        final String timestamp = DateUtils.getMsTimestamp();
        preferences.get(preference).forEach(toolId -> {
            if (isExistingPreferenceValue(toolId, preference, value)) {
                log.info("Got update to tool ID {} preference {}, but that preference is already set to value, not notifying anyone: {}", toolId, preference, value);
                return;
            }
            log.info("Preference {} in tool {} was updated to: {}", preference, toolId, value);
            addExistingPreferenceValue(toolId, preference, value);
            template.convertAndSend(distEventsTopic,
                                    XnatPreferenceUpdateMessage.builder()
                                                               .originatingNodeId(nodeId)
                                                               .timestamp(timestamp)
                                                               .toolId(toolId)
                                                               .preference(preference)
                                                               .value(value)
                                                               .build(),
                                    POST_PROCESSOR);
            log.debug("{}: sent message with timestamp '{}', tool ID '{}', preference '{}', and value {}", nodeId, timestamp, toolId, preference, value);
        });
    }

    public boolean isExistingPreferenceValue(final String toolId, final String preference, final String value) {
        final String mangledKey = mangleKey(toolId, preference);
        return existingPreferences.containsKey(mangledKey) && StringUtils.equals(value, existingPreferences.get(mangledKey));
    }

    public void addExistingPreferenceValue(final String toolId, final String preference, final String value) {
        existingPreferences.put(mangleKey(toolId, preference), value);
    }

    private Map<String, Set<String>> invertPreferenceMap(Map<String, Set<String>> originalMap) {
        Map<String, Set<String>> result = new ConcurrentHashMap<>();

        originalMap.forEach((key, valueList) -> {
            for (String value : valueList) {
                result.computeIfAbsent(value, k -> new HashSet<>()).add(key);
            }
        });

        return result;
    }

    private static String mangleKey(final String toolId, final String preference) {
        return toolId + "::" + preference;
    }
}
