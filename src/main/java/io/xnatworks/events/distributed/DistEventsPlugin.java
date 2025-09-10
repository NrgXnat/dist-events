/*
 * Distributed Events Plugin: io.xnatworks.events.distributed.DistEventsPlugin
 * XNAT http://www.xnat.org
 * Copyright (c) 2025, XNAT Works, Inc.
 * All Rights Reserved
 *
 * Released under the Simplified BSD.
 */

package io.xnatworks.events.distributed;

import lombok.extern.slf4j.Slf4j;
import org.nrg.framework.annotations.XnatPlugin;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.jms.config.DefaultJmsListenerContainerFactory;
import org.springframework.util.ErrorHandler;

import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.Session;
import javax.jms.Topic;

@Slf4j
@XnatPlugin(value = "DistEventsPlugin", name = "Distributed Events Plugin",
            description = "Enables XNAT to propagate events across multiple nodes in a distributed configuration",
            logConfigurationFile = "dist-events-logback.xml", openUrls = "/xapi/dist-events")
@ComponentScan({"io.xnatworks.events.distributed.api", "io.xnatworks.events.distributed.components"})
public class DistEventsPlugin {
    public static final String DIST_EVENTS_TOPIC = "dist-events";

    private final ConnectionFactory connectionFactory;

    @Autowired
    public DistEventsPlugin(final ConnectionFactory connectionFactory) {
        this.connectionFactory = connectionFactory;
    }

    @Bean
    public DefaultJmsListenerContainerFactory jmsTopicListenerContainerFactory(final ErrorHandler errorHandler) {
        final DefaultJmsListenerContainerFactory factory = new DefaultJmsListenerContainerFactory();
        factory.setConnectionFactory(connectionFactory);
        factory.setErrorHandler(errorHandler);
        factory.setConcurrency("1");
        factory.setSessionAcknowledgeMode(Session.AUTO_ACKNOWLEDGE);
        factory.setPubSubDomain(true);
        return factory;
    }

    @Bean
    public Topic distEventsTopic() throws JMSException {
        return connectionFactory.createConnection().createSession(false, Session.AUTO_ACKNOWLEDGE).createTopic(DIST_EVENTS_TOPIC);
    }
}
