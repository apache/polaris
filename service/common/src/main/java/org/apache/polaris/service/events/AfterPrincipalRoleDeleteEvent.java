package org.apache.polaris.service.events;

import org.apache.polaris.core.auth.AuthenticatedPolarisPrincipal;

/**
 * Event fired after a principal role is deleted in Polaris.
 */
public class AfterPrincipalRoleDeleteEvent {
    private final String principalRoleName;
    private final String requestId;
    private final AuthenticatedPolarisPrincipal principal;

    /**
     * Constructs a new AfterPrincipalRoleDeleteEvent.
     *
     * @param principalRoleName the name of the principal role that was deleted
     * @param requestId         the request ID for this operation
     * @param principal         the authenticated principal performing the operation
     */
    public AfterPrincipalRoleDeleteEvent(String principalRoleName, String requestId, AuthenticatedPolarisPrincipal principal) {
        this.principalRoleName = principalRoleName;
        this.requestId = requestId;
        this.principal = principal;
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

