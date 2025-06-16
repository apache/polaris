package org.apache.polaris.service.events;

import org.apache.polaris.core.admin.model.PrincipalRoles;
import org.apache.polaris.core.auth.AuthenticatedPolarisPrincipal;

/**
 * Event fired after the list of principal roles assigned to a principal is retrieved in Polaris.
 */
public class AfterPrincipalRolesAssignedListEvent implements PolarisEvent {
    private final String principalName;
    private final String requestId;
    private final AuthenticatedPolarisPrincipal principal;

    /**
     * Constructs a new AfterPrincipalRolesAssignedListEvent.
     *
     * @param principalName  the name of the principal
     * @param requestId      the request ID for this operation
     * @param principal      the authenticated principal performing the operation
     */
    public AfterPrincipalRolesAssignedListEvent(String principalName, String requestId, AuthenticatedPolarisPrincipal principal) {
        this.principalName = principalName;
        this.requestId = requestId;
        this.principal = principal;
    }

    public String getPrincipalName() {
        return principalName;
    }

    public String getRequestId() {
        return requestId;
    }

    public AuthenticatedPolarisPrincipal getPrincipal() {
        return principal;
    }
}
