package org.apache.polaris.service.events;

import org.apache.polaris.core.admin.model.AddGrantRequest;
import org.apache.polaris.core.auth.AuthenticatedPolarisPrincipal;
import org.apache.polaris.core.entity.PolarisPrivilege;

/**
 * Event fired before a grant is added to a catalog role in Polaris.
 */
public class BeforeAddGrantToCatalogRoleEvent implements PolarisEvent {
    private final String catalogName;
    private final String catalogRoleName;
    private final AddGrantRequest grantRequest;
    private final String requestId;
    private final AuthenticatedPolarisPrincipal principal;

    /**
     * Constructs a new BeforeAddGrantToCatalogRoleEvent.
     *
     * @param catalogName     the name of the catalog
     * @param catalogRoleName the name of the catalog role
     * @param grantRequest    the grant request
     * @param requestId       the request ID for this operation
     * @param principal       the authenticated principal performing the operation
     */
    public BeforeAddGrantToCatalogRoleEvent(String catalogName, String catalogRoleName, AddGrantRequest grantRequest, String requestId, AuthenticatedPolarisPrincipal principal) {
        this.catalogName = catalogName;
        this.catalogRoleName = catalogRoleName;
        this.grantRequest = grantRequest;
        this.requestId = requestId;
        this.principal = principal;
    }

    public String getCatalogName() {
        return catalogName;
    }

    public String getCatalogRoleName() {
        return catalogRoleName;
    }

    public AddGrantRequest getGrantRequest() {
        return grantRequest;
    }

    public String getRequestId() {
        return requestId;
    }

    public AuthenticatedPolarisPrincipal getPrincipal() {
        return principal;
    }
}
