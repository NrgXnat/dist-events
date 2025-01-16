package io.xnatworks.events.distributed.publisher;

import lombok.Builder;
import lombok.Value;

import java.io.Serializable;

@Value
@Builder
public class EventMessage implements Serializable {
    private static final long serialVersionUID = 8689957393358221558L;
    
    String originatingNodeId;
    String timestamp;
    String message;
}
