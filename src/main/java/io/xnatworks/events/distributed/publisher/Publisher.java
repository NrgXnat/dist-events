package io.xnatworks.events.distributed.publisher;

import io.xnatworks.events.distributed.DistEventsPlugin;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomStringUtils;
import org.nrg.xft.utils.DateUtils;
import org.nrg.xnat.services.XnatAppInfo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class Publisher {
    private final JmsTemplate template;
    private final String      nodeId;

    @Autowired
    public Publisher(final JmsTemplate template, final XnatAppInfo appInfo) {
        this.template = template;
        this.nodeId   = appInfo.getNode().getNodeId();
    }

    public EventMessage sendMessage() {
        return sendMessage(RandomStringUtils.randomAlphanumeric(20));
    }

    public EventMessage sendMessage(final String message) {
        final EventMessage eventMessage = EventMessage.builder().originatingNodeId(nodeId).timestamp(DateUtils.getMsTimestamp()).message(message).build();
        template.convertAndSend(DistEventsPlugin.TOPIC_NAME, eventMessage);
        log.debug("{}: sent message with timestamp '{}' and text: '{}'", nodeId, eventMessage.getTimestamp(), eventMessage.getMessage());
        return eventMessage;
    }
}
