package io.xnatworks.events.distributed.components;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.SuperBuilder;
import org.nrg.xft.event.XftItemEvent;
import org.nrg.xft.event.XftItemEventI;

import java.util.List;

@ApiModel(description = "Supports propagating internal XFT item events as distributed event messages.", parent = AbstractEventMessage.class)
@Data
@SuperBuilder
@EqualsAndHashCode(callSuper = true)
public class MultiNodeXftUpdateMessage extends AbstractEventMessage {
    private static final long serialVersionUID = 3847870768722635335L;

    @ApiModelProperty("Indicates the action that was taken on the item.")
    private final String action;

    @ApiModelProperty("Indicates the XSI type of the affected item(s).")
    private final String xsiType;

    @ApiModelProperty("The ID(s) of the affected item(s).")
    private final List<String> ids;

    public XftItemEventI toXftItemEvent() {
        return ids.size() == 1 ? new XftItemEvent(xsiType, ids.get(0), action) : new XftItemEvent(xsiType, ids, action);
    }
}
