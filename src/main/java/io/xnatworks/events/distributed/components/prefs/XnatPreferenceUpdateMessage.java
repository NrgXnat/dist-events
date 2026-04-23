package io.xnatworks.events.distributed.components.prefs;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import io.xnatworks.events.distributed.components.AbstractEventMessage;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import java.io.Serial;

@ApiModel(description = "Supports propagating XNAT preference update events as distributed event messages.", parent = AbstractEventMessage.class)
@Getter
@SuperBuilder
@EqualsAndHashCode(callSuper = true)
@ToString
public class XnatPreferenceUpdateMessage extends AbstractEventMessage {
    @Serial
    private static final long serialVersionUID = -8463181006566256033L;

    @ApiModelProperty("Indicates the ID of the updated preference's tool.")
    private final String toolId;

    @ApiModelProperty("Indicates the name of the updated preference.")
    private final String preference;

    @ApiModelProperty("The new value of the updated preference.")
    private final String value;
}
