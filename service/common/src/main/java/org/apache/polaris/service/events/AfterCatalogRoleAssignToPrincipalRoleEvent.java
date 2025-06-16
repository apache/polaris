package org.apache.polaris.service.events;

import org.apache.polaris.core.auth.AuthenticatedPolarisPrincipal;

/**
 * Event fired after a catalog role is assigned to a principal role in Polaris.
 */
public class AfterCatalogRoleAssignToPrincipalRoleEvent {
    private final String principalRoleName;
    private final String catalogName;
    private final String catalogRoleName;
    private final String requestId;
    private final AuthenticatedPolarisPrincipal principal;

    /**
     * Constructs a new AfterCatalogRoleAssignToPrincipalRoleEvent.
     *
     * @param principalRoleName the name of the principal role
     * @param catalogName       the name of the catalog
     * @param catalogRoleName   the name of the catalog role assigned
     * @param requestId         the request ID for this operation
     * @param principal         the authenticated principal performing the operation
     */
    public AfterCatalogRoleAssignToPrincipalRoleEvent(String principalRoleName, String catalogName, String catalogRoleName, String requestId, AuthenticatedPolarisPrincipal principal) {
        this.principalRoleName = principalRoleName;
        this.catalogName = catalogName;
        this.catalogRoleName = catalogRoleName;
        this.requestId = requestId;
        this.principal = principal;
    }

    public String getPrincipalRoleName() {
        return principalRoleName;
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

