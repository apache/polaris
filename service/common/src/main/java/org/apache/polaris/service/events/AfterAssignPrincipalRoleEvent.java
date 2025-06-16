package org.apache.polaris.service.events;

import org.apache.polaris.core.auth.AuthenticatedPolarisPrincipal;

/**
 * Event fired after a principal role is assigned to a principal in Polaris.
 */
public class AfterAssignPrincipalRoleEvent implements PolarisEvent {
    private final String principalName;
    private final String principalRoleName;
    private final String requestId;
    private final AuthenticatedPolarisPrincipal principal;

    /**
     * Constructs a new AfterAssignPrincipalRoleEvent.
     *
     * @param principalName      the name of the principal
     * @param principalRoleName  the name of the principal role assigned
     * @param requestId          the request ID for this operation
     * @param principal          the authenticated principal performing the operation
     */
    public AfterAssignPrincipalRoleEvent(String principalName, String principalRoleName, String requestId, AuthenticatedPolarisPrincipal principal) {
        this.principalName = principalName;
        this.principalRoleName = principalRoleName;
        this.requestId = requestId;
        this.principal = principal;
    }

    public String getPrincipalName() {
        return principalName;
    }

    public String getPrincipalRoleName() {
        return principalRoleName;
    }

    public String getRequestId() {
        return requestId;
    }

    public AuthenticatedPolarisPrincipal getPrincipal() {
        return principal;
    }
}
