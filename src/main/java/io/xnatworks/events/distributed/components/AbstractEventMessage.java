package io.xnatworks.events.distributed.components;

import io.swagger.annotations.ApiModel;
import io.xnatworks.events.distributed.components.hibernate.HibernateEntityUpdateMessage;
import io.xnatworks.events.distributed.components.xft.MultiNodeXftUpdateMessage;
import lombok.Data;
import lombok.experimental.SuperBuilder;

import java.io.Serializable;

@ApiModel(description = "Provides the basic functionality for a distributed event message.", subTypes = {HibernateEntityUpdateMessage.class, MultiNodeXftUpdateMessage.class})
@Data
@SuperBuilder
public class AbstractEventMessage implements Serializable {
    private static final long serialVersionUID = -1123665247750598419L;

    private final String originatingNodeId;
    private final String timestamp;
}
