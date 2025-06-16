package org.apache.polaris.service.events;

import org.apache.polaris.core.admin.model.CatalogRoles;
import org.apache.polaris.core.auth.AuthenticatedPolarisPrincipal;

/**
 * Event fired after the list of catalog roles is retrieved in Polaris.
 */
public class AfterCatalogRolesListEvent {
    private final String catalogName;
    private final String requestId;
    private final AuthenticatedPolarisPrincipal principal;

    /**
     * Constructs a new AfterCatalogRolesListEvent.
     *
     * @param catalogName  the name of the catalog
     * @param requestId    the request ID for this operation
     * @param principal    the authenticated principal performing the operation
     */
    public AfterCatalogRolesListEvent(String catalogName, String requestId, AuthenticatedPolarisPrincipal principal) {
        this.catalogName = catalogName;
        this.requestId = requestId;
        this.principal = principal;
    }

    public String getCatalogName() {
        return catalogName;
    }

    public String getRequestId() {
        return requestId;
    }

    public AuthenticatedPolarisPrincipal getPrincipal() {
        return principal;
    }
}

