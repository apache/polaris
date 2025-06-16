package org.apache.polaris.service.events;

import org.apache.polaris.core.auth.AuthenticatedPolarisPrincipal;

/**
 * Event fired before a catalog role is deleted in Polaris.
 */
public class BeforeCatalogRoleDeleteEvent implements PolarisEvent {
    private final String catalogName;
    private final String catalogRoleName;
    private final String requestId;
    private final AuthenticatedPolarisPrincipal principal;

    /**
     * Constructs a new BeforeCatalogRoleDeleteEvent.
     *
     * @param catalogName      the name of the catalog
     * @param catalogRoleName  the name of the catalog role to be deleted
     * @param requestId        the request ID for this operation
     * @param principal        the authenticated principal performing the operation
     */
    public BeforeCatalogRoleDeleteEvent(String catalogName, String catalogRoleName, String requestId, AuthenticatedPolarisPrincipal principal) {
        this.catalogName = catalogName;
        this.catalogRoleName = catalogRoleName;
        this.requestId = requestId;
        this.principal = principal;
    }

    public String getCatalogName() {
        return catalogName;
    }

    public String getCatalogRoleName() {
        return catalogRoleName;
    }

    public String getRequestId() {
        return requestId;
    }

    public AuthenticatedPolarisPrincipal getPrincipal() {
        return principal;
    }
}
