package io.xnatworks.events.distributed.components;

import io.xnatworks.events.distributed.DistEventsPlugin;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.nrg.xft.event.XftItemEventI;
import org.nrg.xft.event.methods.XftItemEventHandlerMethod;
import org.nrg.xnat.services.XnatAppInfo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jms.annotation.JmsListener;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@Slf4j
public class MultiNodeXftUpdateListener {
    private final List<XftItemEventHandlerMethod> handlers;
    private final String                          nodeId;

    @Autowired
    public MultiNodeXftUpdateListener(final List<XftItemEventHandlerMethod> handlers, final XnatAppInfo appInfo) {
        this.handlers = handlers;
        this.nodeId   = appInfo.getNode().getNodeId();
    }

    @JmsListener(destination = DistEventsPlugin.DIST_EVENTS_TOPIC, containerFactory = "jmsTopicListenerContainerFactory")
    public void receive(final MultiNodeXftUpdateMessage message) {
        if (StringUtils.equals(nodeId, message.getOriginatingNodeId())) {
            log.info("Received message from this node so basically ignoring it: [{}] action '{}', XSI type '{}', ID(s) '{}'", message.getTimestamp(), message.getAction(), message.getXsiType(), message.getIds());
            return;
        }

        log.info("Received message from node ID {}: [{}] action '{}', XSI type '{}', ID(s) '{}'", message.getOriginatingNodeId(), message.getTimestamp(), message.getAction(), message.getXsiType(), message.getIds());
        final XftItemEventI event = message.toXftItemEvent();
        handlers.stream()
                .filter(handler -> {
                    if (StringUtils.equals("MultiNodeXftUpdateHandlerMethod", handler.getName())) {
                        log.debug("Skipping MultiNodeXftUpdateHandlerMethod handler for event {}", event);
                        return false;
                    }
                    final boolean matches = handler.matches(event);
                    log.debug("Handler {} for event {} {} the event criteria", handler.getName(), event, matches ? "matches" : "does not match");
                    return matches;
                })
                .forEach(handler -> {
                    log.debug("Calling matching handler {} for event {}", handler.getName(), event);
                    handler.handleEvent(event);
                });
    }
}
