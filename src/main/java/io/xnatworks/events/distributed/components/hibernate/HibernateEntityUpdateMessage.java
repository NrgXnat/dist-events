package io.xnatworks.events.distributed.components.hibernate;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import io.xnatworks.events.distributed.components.AbstractEventMessage;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.SuperBuilder;

@ApiModel(description = "Supports propagating internal Hibernate modification events as distributed event messages.", parent = AbstractEventMessage.class)
@Data
@SuperBuilder
@EqualsAndHashCode(callSuper = true)
public class HibernateEntityUpdateMessage extends AbstractEventMessage {
    private static final long serialVersionUID = 6105362581126233163L;

    @ApiModelProperty("Indicates the action that was taken on the entity.")
    private final String action;

    @ApiModelProperty("Indicates the entity type (i.e. class name) of the affected entity.")
    private final String entityType;

    @ApiModelProperty("The ID of the affected entity.")
    private final long id;
}
