package org.apache.polaris.service.events;

import org.apache.polaris.core.admin.model.CatalogRole;
import org.apache.polaris.core.admin.model.UpdateCatalogRoleRequest;
import org.apache.polaris.core.auth.AuthenticatedPolarisPrincipal;

/**
 * Event fired before a catalog role is updated in Polaris.
 */
public class BeforeCatalogRoleUpdateEvent implements PolarisEvent {
    private final String catalogName;
    private final String catalogRoleName;
    private final UpdateCatalogRoleRequest updateRequest;
    private final String requestId;
    private final AuthenticatedPolarisPrincipal principal;

    /**
     * Constructs a new BeforeCatalogRoleUpdateEvent.
     *
     * @param catalogName      the name of the catalog
     * @param catalogRoleName  the name of the catalog role to be updated
     * @param updateRequest    the update request object
     * @param requestId        the request ID for this operation
     * @param principal        the authenticated principal performing the operation
     */
    public BeforeCatalogRoleUpdateEvent(String catalogName, String catalogRoleName, UpdateCatalogRoleRequest updateRequest, String requestId, AuthenticatedPolarisPrincipal principal) {
        this.catalogName = catalogName;
        this.catalogRoleName = catalogRoleName;
        this.updateRequest = updateRequest;
        this.requestId = requestId;
        this.principal = principal;
    }

    public String getCatalogName() {
        return catalogName;
    }

    public String getCatalogRoleName() {
        return catalogRoleName;
    }

    public UpdateCatalogRoleRequest getUpdateRequest() {
        return updateRequest;
    }

    public String getRequestId() {
        return requestId;
    }

    public AuthenticatedPolarisPrincipal getPrincipal() {
        return principal;
    }
}
