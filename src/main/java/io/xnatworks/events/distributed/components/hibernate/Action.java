package io.xnatworks.events.distributed.components.hibernate;

import lombok.Getter;
import org.hibernate.event.spi.EventType;

public enum Action {
    INSERT(EventType.POST_INSERT.eventName(), "inserted"),
    UPDATE(EventType.POST_UPDATE.eventName(), "updated"),
    DELETE(EventType.POST_DELETE.eventName(), "deleted");

    @Getter
    private final String action;
    @Getter
    private final String verb;

    Action(final String action, final String verb) {
        this.action = action;
        this.verb   = verb;
    }
}
