package io.xnatworks.events.distributed.components;

import io.xnatworks.events.distributed.DistEventsPlugin;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.nrg.xdat.om.XnatExperimentdata;
import org.nrg.xdat.om.XnatProjectdata;
import org.nrg.xdat.om.XnatSubjectdata;
import org.nrg.xft.XFTItem;
import org.nrg.xft.event.XftItemEvent;
import org.nrg.xft.event.XftItemEventI;
import org.nrg.xft.event.methods.AbstractXftItemEventHandlerMethod;
import org.nrg.xft.event.methods.XftItemEventCriteria;
import org.nrg.xft.exception.ElementNotFoundException;
import org.nrg.xft.exception.FieldNotFoundException;
import org.nrg.xft.exception.XFTInitException;
import org.nrg.xft.utils.DateUtils;
import org.nrg.xnat.services.XnatAppInfo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Conditional;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

@Component
@Conditional(IsMultiNodeDeployment.class)
@Slf4j
public class MultiNodeXftUpdateHandlerMethod extends AbstractXftItemEventHandlerMethod {
    private static final List<XftItemEventCriteria> CRITERIA = Arrays.asList(XftItemEventCriteria.builder().xsiType(XnatProjectdata.SCHEMA_ELEMENT_NAME).actions(XftItemEvent.CREATE, XftItemEvent.DELETE).build(),
                                                                             XftItemEventCriteria.builder().xsiType(XnatSubjectdata.SCHEMA_ELEMENT_NAME).actions(XftItemEvent.CREATE, XftItemEvent.DELETE).build(),
                                                                             XftItemEventCriteria.builder().xsiType(XnatExperimentdata.SCHEMA_ELEMENT_NAME).actions(XftItemEvent.CREATE, XftItemEvent.DELETE).build());

    private final JmsTemplate template;
    private final String      nodeId;

    @Autowired
    public MultiNodeXftUpdateHandlerMethod(final JmsTemplate template, final XnatAppInfo appInfo) {
        super(CRITERIA);
        this.template = template;
        this.nodeId   = appInfo.getNode().getNodeId();
    }

    @Override
    protected boolean handleEventImpl(final XftItemEventI event) {
        final String       action  = event.getAction();
        final String       xsiType = event.getXsiType();
        final List<String> ids     = getIds(event);

        if (StringUtils.equalsAny(action, XftItemEvent.CREATE, XftItemEvent.DELETE)) {
            log.warn("This handler only handles create and delete events, but got unsupported event action {} for type {}: {}", event.getAction(), event.getXsiType(), getIds(event));
            return false;
        }

        final String timestamp = DateUtils.getMsTimestamp();
        template.convertAndSend(DistEventsPlugin.DIST_EVENTS_TOPIC, MultiNodeXftUpdateMessage.builder()
                                                                                             .originatingNodeId(nodeId)
                                                                                             .timestamp(timestamp)
                                                                                             .action(action)
                                                                                             .xsiType(xsiType)
                                                                                             .ids(ids)
                                                                                             .build());
        log.debug("{}: sent message with timestamp '{}', action '{}', xsiType '{}' for ID(s) '{}'", nodeId, timestamp, action, xsiType, ids);
        return true;
    }

    private static List<String> getIds(final XftItemEventI event) {
        if (event.isMultiItemEvent()) {
            if (!CollectionUtils.isEmpty(event.getIds())) {
                return event.getIds();
            }
            if (!CollectionUtils.isEmpty(event.getItems())) {
                return event.getItems()
                            .stream()
                            .map(MultiNodeXftUpdateHandlerMethod::getXftItemId)
                            .filter(Objects::nonNull)
                            .collect(Collectors.toList());
            }
        }
        return Collections.singletonList(StringUtils.isNotBlank(event.getId()) ? event.getId() : getXftItemId(event.getItem()));
    }

    private static String getXftItemId(final XFTItem item) {
        try {
            return item.getStringProperty("ID");
        } catch (XFTInitException | ElementNotFoundException | FieldNotFoundException e) {
            log.error("An error occurred trying to retrieve the ID for an XFT item of type {}", item.getXSIType(), e);
            return null;
        }
    }
}
