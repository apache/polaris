package org.apache.polaris.service.events;

import org.apache.polaris.core.admin.model.PrincipalRole;
import org.apache.polaris.core.admin.model.UpdatePrincipalRoleRequest;
import org.apache.polaris.core.auth.AuthenticatedPolarisPrincipal;

/**
 * Event fired before a principal role is updated in Polaris.
 */
public class BeforePrincipalRoleUpdateEvent implements PolarisEvent {
    private final String principalRoleName;
    private final UpdatePrincipalRoleRequest updateRequest;
    private final String requestId;
    private final AuthenticatedPolarisPrincipal principal;

    /**
     * Constructs a new BeforePrincipalRoleUpdateEvent.
     *
     * @param principalRoleName the name of the principal role to be updated
     * @param updateRequest     the update request object
     * @param requestId         the request ID for this operation
     * @param principal         the authenticated principal performing the operation
     */
    public BeforePrincipalRoleUpdateEvent(String principalRoleName, UpdatePrincipalRoleRequest updateRequest, String requestId, AuthenticatedPolarisPrincipal principal) {
        this.principalRoleName = principalRoleName;
        this.updateRequest = updateRequest;
        this.requestId = requestId;
        this.principal = principal;
    }

    public String getPrincipalRoleName() {
        return principalRoleName;
    }

    public UpdatePrincipalRoleRequest getUpdateRequest() {
        return updateRequest;
    }

    public String getRequestId() {
        return requestId;
    }

    public AuthenticatedPolarisPrincipal getPrincipal() {
        return principal;
    }
}
