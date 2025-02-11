package io.xnatworks.events.distributed.components;

import lombok.Data;
import lombok.experimental.SuperBuilder;

import java.io.Serializable;

@Data
@SuperBuilder
public class AbstractEventMessage implements Serializable {
    private static final long serialVersionUID = -1123665247750598419L;

    private final String originatingNodeId;
    private final String timestamp;
}
