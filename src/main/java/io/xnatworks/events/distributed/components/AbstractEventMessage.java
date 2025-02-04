package io.xnatworks.events.distributed.components;

import lombok.Data;
import lombok.experimental.SuperBuilder;

@Data
@SuperBuilder
public class AbstractEventMessage {
    private final String originatingNodeId;
    private final String timestamp;
}
