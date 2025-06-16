package org.apache.polaris.service.events;

import org.apache.polaris.core.admin.model.PrincipalRoles;
import org.apache.polaris.core.auth.AuthenticatedPolarisPrincipal;

/**
 * Event fired after the list of principal roles is retrieved in Polaris.
 */
public class AfterPrincipalRolesListEvent {
    private final String requestId;
    private final AuthenticatedPolarisPrincipal principal;
    /**
     * Constructs a new AfterPrincipalRolesListEvent.
     *
     * @param requestId      the request ID for this operation
     * @param principal      the authenticated principal performing the operation
     */
    public AfterPrincipalRolesListEvent(String requestId, AuthenticatedPolarisPrincipal principal) {
        this.requestId = requestId;
        this.principal = principal;
    }

    public String getRequestId() {
        return requestId;
    }

    public AuthenticatedPolarisPrincipal getPrincipal() {
        return principal;
    }
}

