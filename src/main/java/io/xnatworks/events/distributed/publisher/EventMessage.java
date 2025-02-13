package io.xnatworks.events.distributed.publisher;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import io.xnatworks.events.distributed.components.AbstractEventMessage;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.SuperBuilder;

import java.io.Serializable;

@ApiModel(description = "Provides the simplest implementation of a distributed event message.", parent = AbstractEventMessage.class)
@Data
@SuperBuilder
@EqualsAndHashCode(callSuper = true)
public class EventMessage extends AbstractEventMessage implements Serializable {
    private static final long serialVersionUID = 8689957393358221558L;

    @ApiModelProperty("Specifies the message text.")
    String message;
}
