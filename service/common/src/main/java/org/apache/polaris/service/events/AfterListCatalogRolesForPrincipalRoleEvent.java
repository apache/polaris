package org.apache.polaris.service.events;

import org.apache.polaris.core.auth.AuthenticatedPolarisPrincipal;

/** Event fired after the list of catalog roles for a principal role is retrieved in Polaris. */
public class AfterListCatalogRolesForPrincipalRoleEvent implements PolarisEvent {
  private final String principalRoleName;
  private final String catalogName;
  private final String requestId;
  private final AuthenticatedPolarisPrincipal principal;

  /**
   * Constructs a new AfterListCatalogRolesForPrincipalRoleEvent.
   *
   * @param principalRoleName the name of the principal role
   * @param catalogName the name of the catalog
   * @param requestId the request ID for this operation
   * @param principal the authenticated principal performing the operation
   */
  public AfterListCatalogRolesForPrincipalRoleEvent(
      String principalRoleName,
      String catalogName,
      String requestId,
      AuthenticatedPolarisPrincipal principal) {
    this.principalRoleName = principalRoleName;
    this.catalogName = catalogName;
    this.requestId = requestId;
    this.principal = principal;
  }

  public String getPrincipalRoleName() {
    return principalRoleName;
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
