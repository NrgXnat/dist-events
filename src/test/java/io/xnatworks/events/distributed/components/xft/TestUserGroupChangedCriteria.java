package io.xnatworks.events.distributed.components.xft;

import org.junit.jupiter.api.Test;
import org.nrg.xdat.om.XdatUsergroup;
import org.nrg.xft.event.XftItemEvent;
import org.nrg.xft.event.XftItemEventI;

import java.util.Collections;

import static io.xnatworks.events.distributed.components.xft.MultiNodeXftUpdateHandlerMethod.CRITERIA_USERGROUP_CHANGED;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.nrg.xdat.security.helpers.Groups.OPERATION_ADD_USERS;
import static org.nrg.xdat.security.helpers.Groups.OPERATION_REMOVE_USERS;
import static org.nrg.xdat.security.helpers.Groups.USERS;
import static org.nrg.xft.event.XftItemEventI.OPERATION;

/**
 * Which user-group events are forwarded to other nodes. XNAT adds a group's initial members, such as a new project's
 * creator in its owner group, with a CREATE event and later members with an UPDATE event; both have to be forwarded,
 * or other nodes never see the initial members.
 */
class TestUserGroupChangedCriteria {

    @Test
    void forwardsTheCreateThatAddsAProjectCreatorToItsOwnerGroup() {
        assertTrue(CRITERIA_USERGROUP_CHANGED.matches(groupEvent(XftItemEventI.CREATE, OPERATION_ADD_USERS)));
    }

    @Test
    void stillForwardsUpdatesThatAddOrRemoveUsers() {
        assertTrue(CRITERIA_USERGROUP_CHANGED.matches(groupEvent(XftItemEventI.UPDATE, OPERATION_ADD_USERS)));
        assertTrue(CRITERIA_USERGROUP_CHANGED.matches(groupEvent(XftItemEventI.UPDATE, OPERATION_REMOVE_USERS)));
    }

    @Test
    void ignoresGroupEventsThatDontChangeMembership() {
        assertFalse(CRITERIA_USERGROUP_CHANGED.matches(groupEvent(XftItemEventI.CREATE, null)));
        assertFalse(CRITERIA_USERGROUP_CHANGED.matches(groupEvent(XftItemEventI.UPDATE, null)));
    }

    private static XftItemEventI groupEvent(final String action, final String operation) {
        final XftItemEvent.Builder builder = XftItemEvent.builder().xsiType(XdatUsergroup.SCHEMA_ELEMENT_NAME).id("P1_owner").action(action);
        if (operation != null) {
            builder.property(OPERATION, operation).property(USERS, Collections.singleton("creator"));
        }
        return builder.build();
    }

}
