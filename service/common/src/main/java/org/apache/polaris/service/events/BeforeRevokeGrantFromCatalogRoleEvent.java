package org.apache.polaris.service.events;

import org.apache.polaris.core.admin.model.RevokeGrantRequest;
import org.apache.polaris.core.auth.AuthenticatedPolarisPrincipal;
import org.apache.polaris.core.entity.PolarisPrivilege;

/**
 * Event fired before a grant is revoked from a catalog role in Polaris.
 */
public class BeforeRevokeGrantFromCatalogRoleEvent implements PolarisEvent {
    private final String catalogName;
    private final String catalogRoleName;
    private final RevokeGrantRequest grantRequest;
    private final Boolean cascade;
    private final String requestId;
    private final AuthenticatedPolarisPrincipal principal;

    /**
     * Constructs a new BeforeRevokeGrantFromCatalogRoleEvent.
     *
     * @param catalogName     the name of the catalog
     * @param catalogRoleName the name of the catalog role
     * @param grantRequest    the revoke grant request
     * @param cascade         whether the revoke is cascading
     * @param requestId       the request ID for this operation
     * @param principal       the authenticated principal performing the operation
     */
    public BeforeRevokeGrantFromCatalogRoleEvent(String catalogName, String catalogRoleName, RevokeGrantRequest grantRequest, Boolean cascade, String requestId, AuthenticatedPolarisPrincipal principal) {
        this.catalogName = catalogName;
        this.catalogRoleName = catalogRoleName;
        this.grantRequest = grantRequest;
        this.cascade = cascade;
        this.requestId = requestId;
        this.principal = principal;
    }

    public String getCatalogName() {
        return catalogName;
    }

    public String getCatalogRoleName() {
        return catalogRoleName;
    }

    public RevokeGrantRequest getGrantRequest() {
        return grantRequest;
    }

    public Boolean getCascade() {
        return cascade;
    }

    public String getRequestId() {
        return requestId;
    }

    public AuthenticatedPolarisPrincipal getPrincipal() {
        return principal;
    }
}
