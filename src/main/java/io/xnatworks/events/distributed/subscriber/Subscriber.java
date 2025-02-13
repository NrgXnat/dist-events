package io.xnatworks.events.distributed.subscriber;

import io.xnatworks.events.distributed.DistEventsPlugin;
import io.xnatworks.events.distributed.publisher.EventMessage;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.nrg.xnat.services.XnatAppInfo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jms.annotation.JmsListener;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class Subscriber {
    private final String nodeId;

    @Autowired
    public Subscriber(final XnatAppInfo appInfo) {
        this.nodeId = appInfo.getNode().getNodeId();
    }

    @JmsListener(destination = DistEventsPlugin.DIST_TEST_TOPIC, containerFactory = "jmsTopicListenerContainerFactory")
    public void receive(final EventMessage message) {
        if (StringUtils.equals(nodeId, message.getOriginatingNodeId())) {
            log.info("Received message from this node so basically ignoring it: [{}] {}", message.getTimestamp(), message.getMessage());
        } else {
            log.info("Received message from node ID {}: [{}] {}", message.getOriginatingNodeId(), message.getTimestamp(), message.getMessage());
        }
    }
}
