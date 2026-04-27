package io.xnatworks.events.distributed.components.xft;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import io.xnatworks.events.distributed.components.AbstractEventMessage;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.apache.commons.lang3.ObjectUtils;
import org.nrg.xft.event.XftItemEvent;
import org.nrg.xft.event.XftItemEventI;

import java.io.Serial;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@ApiModel(description = "Supports propagating internal XFT item events as distributed event messages.", parent = AbstractEventMessage.class)
@Getter
@SuperBuilder
@EqualsAndHashCode(callSuper = true)
@ToString
public class MultiNodeXftUpdateMessage extends AbstractEventMessage {
    @Serial
    private static final long serialVersionUID = 3847870768722635335L;

    @ApiModelProperty("Indicates the action that was taken on the item.")
    private final String action;

    @ApiModelProperty("Indicates the XSI type of the affected item(s).")
    private final String xsiType;

    @ApiModelProperty("The ID(s) of the affected item(s).")
    private final List<String> ids;

    @ApiModelProperty("The properties of the event.")
    private final Map<String, ?> properties;

    public XftItemEventI toXftItemEvent() {
        return ids.size() == 1 ? XftItemEvent.builder().xsiType(xsiType).id(ids.getFirst()).action(action).properties(ObjectUtils.defaultIfNull(properties, Collections.emptyMap())).build()
                               : XftItemEvent.builder().xsiType(xsiType).ids(ids).action(action).properties(ObjectUtils.defaultIfNull(properties, Collections.emptyMap())).build();
    }
}
