package org.apache.polaris.service.events;

import org.apache.polaris.core.auth.AuthenticatedPolarisPrincipal;

/**
 * Event fired before the list of principal roles is retrieved in Polaris.
 */
public class BeforePrincipalRolesListEvent {
    private final String requestId;
    private final AuthenticatedPolarisPrincipal principal;

    /**
     * Constructs a new BeforePrincipalRolesListEvent.
     *
     * @param requestId the request ID for this operation
     * @param principal the authenticated principal performing the operation
     */
    public BeforePrincipalRolesListEvent(String requestId, AuthenticatedPolarisPrincipal principal) {
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

