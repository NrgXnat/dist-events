package io.xnatworks.events.distributed.components;

import lombok.extern.slf4j.Slf4j;
import org.springframework.jms.core.JmsTemplate;

import javax.jms.ConnectionFactory;

@Slf4j
public class JmsTopicTemplate extends JmsTemplate {
    public JmsTopicTemplate(final ConnectionFactory connectionFactory) {
        super(connectionFactory);
        setPubSubDomain(true);
        setPubSubNoLocal(true);
    }
}
