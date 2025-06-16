package org.apache.polaris.service.events;

import org.apache.polaris.core.auth.AuthenticatedPolarisPrincipal;

/**
 * Event fired before a principal is deleted in Polaris.
 */
public class BeforePrincipalDeleteEvent implements PolarisEvent {
    private final String principalName;
    private final String requestId;
    private final AuthenticatedPolarisPrincipal principal;

    /**
     * Constructs a new BeforePrincipalDeleteEvent.
     *
     * @param principalName the name of the principal to be deleted
     * @param requestId     the request ID for this operation
     * @param principal     the authenticated principal performing the operation
     */
    public BeforePrincipalDeleteEvent(String principalName, String requestId, AuthenticatedPolarisPrincipal principal) {
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
