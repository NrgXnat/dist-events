package io.xnatworks.events.distributed.components;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.SuperBuilder;
import org.nrg.xft.event.XftItemEvent;
import org.nrg.xft.event.XftItemEventI;

import java.util.List;

@Data
@SuperBuilder
@EqualsAndHashCode(callSuper = true)
public class MultiNodeXftUpdateMessage extends AbstractEventMessage {
    private static final long serialVersionUID = 3847870768722635335L;

    private final String       action;
    private final String       xsiType;
    private final List<String> ids;

    public XftItemEventI toXftItemEvent() {
        return ids.size() == 1 ? new XftItemEvent(xsiType, ids.get(0), action) : new XftItemEvent(xsiType, ids, action);
    }
}
