package io.xnatworks.events.distributed.publisher;

import io.xnatworks.events.distributed.components.AbstractEventMessage;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.SuperBuilder;

import java.io.Serializable;

@Data
@SuperBuilder
@EqualsAndHashCode(callSuper = true)
public class EventMessage extends AbstractEventMessage implements Serializable {
    private static final long serialVersionUID = 8689957393358221558L;

    String message;
}
