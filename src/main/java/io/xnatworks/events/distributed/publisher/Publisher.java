package io.xnatworks.events.distributed.publisher;

import io.xnatworks.events.distributed.components.IsMultiNodeDeployment;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomStringUtils;
import org.nrg.xft.utils.DateUtils;
import org.nrg.xnat.services.XnatAppInfo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Conditional;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.stereotype.Component;

import javax.jms.ConnectionFactory;
import javax.jms.Topic;

@Component
@Conditional(IsMultiNodeDeployment.class)
@Slf4j
public class Publisher {
    private final JmsTemplate template;
    private final Topic       testTopic;
    private final String      nodeId;

    @Autowired
    public Publisher(final XnatAppInfo appInfo, final ConnectionFactory connectionFactory, final Topic testTopic) {
        this.nodeId   = appInfo.getNode().getNodeId();
        this.template = new JmsTemplate(connectionFactory);
        this.template.setPubSubDomain(true);
        this.template.setPubSubNoLocal(true);
        this.testTopic = testTopic;
    }

    public EventMessage sendMessage() {
        return sendMessage(RandomStringUtils.randomAlphanumeric(20));
    }

    public EventMessage sendMessage(final String message) {
        final EventMessage eventMessage = EventMessage.builder().originatingNodeId(nodeId).timestamp(DateUtils.getMsTimestamp()).message(message).build();
        template.convertAndSend(testTopic, eventMessage);
        log.debug("{}: sent message with timestamp '{}' and text: '{}'", nodeId, eventMessage.getTimestamp(), eventMessage.getMessage());
        return eventMessage;
    }
}
