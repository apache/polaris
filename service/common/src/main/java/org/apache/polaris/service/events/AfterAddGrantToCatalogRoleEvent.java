package org.apache.polaris.service.events;

import org.apache.polaris.core.admin.model.AddGrantRequest;
import org.apache.polaris.core.auth.AuthenticatedPolarisPrincipal;
import org.apache.polaris.core.entity.PolarisPrivilege;

/**
 * Event fired after a grant is added to a catalog role in Polaris.
 */
public class AfterAddGrantToCatalogRoleEvent {
    private final String catalogName;
    private final String catalogRoleName;
    private final PolarisPrivilege privilege;
    private final AddGrantRequest grantRequest;
    private final String requestId;
    private final AuthenticatedPolarisPrincipal principal;

    /**
     * Constructs a new AfterAddGrantToCatalogRoleEvent.
     *
     * @param catalogName     the name of the catalog
     * @param catalogRoleName the name of the catalog role
     * @param privilege       the privilege granted
     * @param grantRequest    the grant request
     * @param requestId       the request ID for this operation
     * @param principal       the authenticated principal performing the operation
     */
    public AfterAddGrantToCatalogRoleEvent(String catalogName, String catalogRoleName, PolarisPrivilege privilege, AddGrantRequest grantRequest, String requestId, AuthenticatedPolarisPrincipal principal) {
        this.catalogName = catalogName;
        this.catalogRoleName = catalogRoleName;
        this.privilege = privilege;
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

    public PolarisPrivilege getPrivilege() {
        return privilege;
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

