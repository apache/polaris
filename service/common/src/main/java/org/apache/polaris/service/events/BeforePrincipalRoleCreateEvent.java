package org.apache.polaris.service.events;

import org.apache.polaris.core.auth.AuthenticatedPolarisPrincipal;

/**
 * Event fired before a principal role is created in Polaris.
 */
public class BeforePrincipalRoleCreateEvent {
    private final String principalRoleName;
    private final String requestId;
    private final AuthenticatedPolarisPrincipal principal;

    /**
     * Constructs a new BeforePrincipalRoleCreateEvent.
     *
     * @param principalRoleName the name of the principal role to be created
     * @param requestId         the request ID for this operation
     * @param principal         the authenticated principal performing the operation
     */
    public BeforePrincipalRoleCreateEvent(String principalRoleName, String requestId, AuthenticatedPolarisPrincipal principal) {
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

