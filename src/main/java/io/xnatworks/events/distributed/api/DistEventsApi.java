package io.xnatworks.events.distributed.api;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import lombok.extern.slf4j.Slf4j;
import org.nrg.framework.annotations.XapiRestController;
import org.nrg.xapi.rest.AbstractXapiRestController;
import org.nrg.xapi.rest.XapiRequestMapping;
import org.nrg.xdat.security.helpers.AccessLevel;
import org.nrg.xdat.security.services.RoleHolder;
import org.nrg.xdat.security.services.UserManagementServiceI;
import org.nrg.xnat.services.XnatAppInfo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;

import static org.springframework.http.MediaType.APPLICATION_JSON_VALUE;
import static org.springframework.web.bind.annotation.RequestMethod.GET;

@Api("XNAT Distributed Events API")
@XapiRestController
@RequestMapping(value = "/dist-events")
@Slf4j
public class DistEventsApi extends AbstractXapiRestController {
    private final String  nodeId;
    private final boolean isPrimaryNode;

    @Autowired
    public DistEventsApi(final UserManagementServiceI userManagementService, final RoleHolder roleHolder, final XnatAppInfo appInfo) {
        super(userManagementService, roleHolder);
        this.nodeId        = appInfo.getNode().getNodeId();
        this.isPrimaryNode = appInfo.isPrimaryNode();
    }

    @ApiOperation(value = "Returns the node ID for this system.", response = String.class)
    @ApiResponses({@ApiResponse(code = 200, message = "Node ID successfully retrieved."),
                   @ApiResponse(code = 401, message = "Must be authenticated to access the XNAT REST API."),
                   @ApiResponse(code = 403, message = "Not authorized to set site configuration properties."),
                   @ApiResponse(code = 500, message = "Unexpected error")})
    @XapiRequestMapping(value = "node-id", produces = APPLICATION_JSON_VALUE, method = GET, restrictTo = AccessLevel.Admin)
    public String getNodeId() {
        return nodeId;
    }

    @ApiOperation(value = "Returns whether this system is the primary node.", response = Boolean.class)
    @ApiResponses({@ApiResponse(code = 200, message = "Primary node status successfully retrieved."),
                   @ApiResponse(code = 401, message = "Must be authenticated to access the XNAT REST API."),
                   @ApiResponse(code = 403, message = "Not authorized to set site configuration properties."),
                   @ApiResponse(code = 500, message = "Unexpected error")})
    @XapiRequestMapping(value = "is-primary", produces = APPLICATION_JSON_VALUE, method = GET, restrictTo = AccessLevel.Admin)
    public boolean isPrimaryNode() {
        return isPrimaryNode;
    }
}
