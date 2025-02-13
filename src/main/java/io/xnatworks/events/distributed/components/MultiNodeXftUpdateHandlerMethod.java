package io.xnatworks.events.distributed.components;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.nrg.xdat.om.XdatUser;
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
import org.springframework.jms.core.JmsTemplate;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import javax.jms.ConnectionFactory;
import javax.jms.Topic;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.nrg.xft.event.XftItemEventI.OPERATION;
import static org.nrg.xdat.security.helpers.Roles.ADDED_ROLES;
import static org.nrg.xdat.security.helpers.Roles.DELETED_ROLES;
import static org.nrg.xdat.security.helpers.Roles.OPERATION_ADD_ROLE;
import static org.nrg.xdat.security.helpers.Roles.OPERATION_ADD_ROLES;
import static org.nrg.xdat.security.helpers.Roles.OPERATION_DELETE_ROLE;
import static org.nrg.xdat.security.helpers.Roles.OPERATION_DELETE_ROLES;
import static org.nrg.xdat.security.helpers.Roles.OPERATION_MODIFIED_ROLES;

@Component
@Slf4j
public class MultiNodeXftUpdateHandlerMethod extends AbstractXftItemEventHandlerMethod {
    public static final  Predicate<XftItemEventI>   PREDICATE_ROLES_CHANGED = event -> event.getProperties().containsKey(OPERATION) && StringUtils.equalsAny(event.getProperties().get(OPERATION).toString(), ADDED_ROLES, DELETED_ROLES, OPERATION_ADD_ROLE, OPERATION_ADD_ROLES, OPERATION_DELETE_ROLE, OPERATION_DELETE_ROLES, OPERATION_MODIFIED_ROLES);
    private static final List<XftItemEventCriteria> CRITERIA                = Arrays.asList(XftItemEventCriteria.builder().xsiType(XnatProjectdata.SCHEMA_ELEMENT_NAME).actions(XftItemEvent.CREATE, XftItemEvent.DELETE).build(),
                                                                                            XftItemEventCriteria.builder().xsiType(XnatSubjectdata.SCHEMA_ELEMENT_NAME).actions(XftItemEvent.CREATE, XftItemEvent.DELETE, XftItemEvent.MOVE, XftItemEvent.SHARE).build(),
                                                                                            XftItemEventCriteria.builder().xsiType(XnatExperimentdata.SCHEMA_ELEMENT_NAME).actions(XftItemEvent.CREATE, XftItemEvent.DELETE, XftItemEvent.MOVE, XftItemEvent.SHARE).build(),
                                                                                            XftItemEventCriteria.builder().xsiType(XdatUser.SCHEMA_ELEMENT_NAME).actions(XftItemEvent.UPDATE).predicate(PREDICATE_ROLES_CHANGED).build());

    private final String      nodeId;
    private final JmsTemplate template;
    private final Topic       distEventsTopic;

    @Autowired
    public MultiNodeXftUpdateHandlerMethod(final XnatAppInfo appInfo, final ConnectionFactory connectionFactory, final Topic distEventsTopic) {
        super(CRITERIA);
        this.nodeId   = appInfo.getNode().getNodeId();
        this.template = new JmsTemplate(connectionFactory);
        this.template.setPubSubDomain(true);
        this.template.setPubSubNoLocal(true);
        this.distEventsTopic = distEventsTopic;
    }

    @Override
    protected boolean handleEventImpl(final XftItemEventI event) {
        final String       action  = event.getAction();
        final String       xsiType = event.getXsiType();
        final List<String> ids     = getIds(event);

        log.debug("Handling event action '{}', xsiType '{}' for ID(s) '{}' on node {}", action, xsiType, ids, nodeId);

        final String timestamp = DateUtils.getMsTimestamp();
        template.convertAndSend(distEventsTopic, MultiNodeXftUpdateMessage.builder()
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
