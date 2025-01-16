package io.xnatworks.events.distributed.api;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import io.xnatworks.events.distributed.publisher.EventMessage;
import io.xnatworks.events.distributed.publisher.Publisher;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.nrg.framework.annotations.XapiRestController;
import org.nrg.xapi.rest.AbstractXapiRestController;
import org.nrg.xapi.rest.XapiRequestMapping;
import org.nrg.xdat.security.helpers.AccessLevel;
import org.nrg.xdat.security.services.RoleHolder;
import org.nrg.xdat.security.services.UserManagementServiceI;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;

import static org.springframework.http.MediaType.APPLICATION_JSON_VALUE;
import static org.springframework.web.bind.annotation.RequestMethod.POST;

@Api("XNAT Distributed Events API")
@XapiRestController
@RequestMapping(value = "/dist-events")
@Slf4j
public class DistEventsApi extends AbstractXapiRestController {
    private final Publisher publisher;

    @Autowired
    public DistEventsApi(final UserManagementServiceI userManagementService, final RoleHolder roleHolder, final Publisher publisher) {
        super(userManagementService, roleHolder);
        this.publisher = publisher;
    }

    @ApiOperation(value = "Creates a new \"event\" message and puts it on the topic.", notes = "Returns the newly created message for inspection.", response = EventMessage.class)
    @ApiResponses({@ApiResponse(code = 200, message = "Returns the newly created event message."),
                   @ApiResponse(code = 403, message = "Insufficient privileges to create an event message."),
                   @ApiResponse(code = 500, message = "An unexpected or unknown error occurred.")})
    @XapiRequestMapping(produces = APPLICATION_JSON_VALUE, method = POST, restrictTo = AccessLevel.Admin)
    public EventMessage createEventMessage(@RequestBody(required = false) final String message) {
        return StringUtils.isBlank(message) ? publisher.sendMessage() : publisher.sendMessage(message);
    }
}
