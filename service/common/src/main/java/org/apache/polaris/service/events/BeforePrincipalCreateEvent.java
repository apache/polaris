package org.apache.polaris.service.events;

import org.apache.polaris.core.auth.AuthenticatedPolarisPrincipal;

/**
 * Emitted before a principal is created in Polaris.
 */
public final class BeforePrincipalCreateEvent implements PolarisEvent {
    private final String principalName;
    private final String user;

    public BeforePrincipalCreateEvent(String principalName, AuthenticatedPolarisPrincipal principal) {
        this.principalName = principalName;
        if (principal != null) {
            this.user = principal.getName();
        } else {
            this.user = null;
        }
    }

    public String getPrincipalName() {
        return principalName;
    }

    public String getUser() {
        return user;
    }
}
