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
import org.apache.activemq.command.ActiveMQTopic;
import org.nrg.framework.annotations.XnatPlugin;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;

import javax.jms.Topic;

@Slf4j
@XnatPlugin(value = "DistEventsPlugin", name = "Distributed Events Plugin",
            description = "Enables XNAT to propagate events across multiple nodes in a distributed configuration",
            logConfigurationFile = "dist-events-logback.xml")
@ComponentScan({"io.xnatworks.events.distributed.api", "io.xnatworks.events.distributed.publisher", "io.xnatworks.events.distributed.subscriber"})
public class DistEventsPlugin {
    public static final String TOPIC_NAME = "dist-events";

    @Bean
    public Topic distEventsTopic() {
        return new ActiveMQTopic(TOPIC_NAME);
    }
}
