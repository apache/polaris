package org.apache.polaris.service.events;

import org.apache.polaris.core.admin.model.Principal;
import org.apache.polaris.core.auth.AuthenticatedPolarisPrincipal;

/**
 * Event fired after a principal is updated in Polaris.
 */
public class AfterPrincipalUpdateEvent implements PolarisEvent {
    private final String principalName;
    private final Principal updatedPrincipal;
    private final String requestId;
    private final AuthenticatedPolarisPrincipal principal;

    /**
     * Constructs a new AfterPrincipalUpdateEvent.
     *
     * @param principalName    the name of the principal that was updated
     * @param updatedPrincipal the updated principal object
     * @param requestId        the request ID for this operation
     * @param principal        the authenticated principal performing the operation
     */
    public AfterPrincipalUpdateEvent(String principalName, Principal updatedPrincipal, String requestId, AuthenticatedPolarisPrincipal principal) {
        this.principalName = principalName;
        this.updatedPrincipal = updatedPrincipal;
        this.requestId = requestId;
        this.principal = principal;
    }

    public String getPrincipalName() {
        return principalName;
    }

    public Principal getUpdatedPrincipal() {
        return updatedPrincipal;
    }

    public String getRequestId() {
        return requestId;
    }

    public AuthenticatedPolarisPrincipal getPrincipal() {
        return principal;
    }
}
