package org.apache.polaris.service.events;

import org.apache.polaris.core.admin.model.RevokeGrantRequest;
import org.apache.polaris.core.auth.AuthenticatedPolarisPrincipal;
import org.apache.polaris.core.entity.PolarisPrivilege;

/** Event fired after a grant is revoked from a catalog role in Polaris. */
public class AfterRevokeGrantFromCatalogRoleEvent implements PolarisEvent {
  private final String catalogName;
  private final String catalogRoleName;
  private final PolarisPrivilege privilege;
  private final RevokeGrantRequest grantRequest;
  private final Boolean cascade;
  private final String requestId;
  private final AuthenticatedPolarisPrincipal principal;

  /**
   * Constructs a new AfterRevokeGrantFromCatalogRoleEvent.
   *
   * @param catalogName the name of the catalog
   * @param catalogRoleName the name of the catalog role
   * @param privilege the privilege revoked
   * @param grantRequest the revoke grant request
   * @param requestId the request ID for this operation
   * @param principal the authenticated principal performing the operation
   */
  public AfterRevokeGrantFromCatalogRoleEvent(
      String catalogName,
      String catalogRoleName,
      PolarisPrivilege privilege,
      RevokeGrantRequest grantRequest,
      Boolean cascade,
      String requestId,
      AuthenticatedPolarisPrincipal principal) {
    this.catalogName = catalogName;
    this.catalogRoleName = catalogRoleName;
    this.privilege = privilege;
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

  public PolarisPrivilege getPrivilege() {
    return privilege;
  }

  public RevokeGrantRequest getGrantRequest() {
    return grantRequest;
  }

  public boolean isCascade() {
    return cascade;
  }

  public String getRequestId() {
    return requestId;
  }

  public AuthenticatedPolarisPrincipal getPrincipal() {
    return principal;
  }
}
