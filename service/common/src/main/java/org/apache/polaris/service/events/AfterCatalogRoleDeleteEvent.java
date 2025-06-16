package org.apache.polaris.service.events;

import org.apache.polaris.core.auth.AuthenticatedPolarisPrincipal;

/** Event fired after a catalog role is deleted in Polaris. */
public class AfterCatalogRoleDeleteEvent implements PolarisEvent {
  private final String catalogName;
  private final String catalogRoleName;
  private final String requestId;
  private final AuthenticatedPolarisPrincipal principal;

  /**
   * Constructs a new AfterCatalogRoleDeleteEvent.
   *
   * @param catalogName the name of the catalog
   * @param catalogRoleName the name of the catalog role that was deleted
   * @param requestId the request ID for this operation
   * @param principal the authenticated principal performing the operation
   */
  public AfterCatalogRoleDeleteEvent(
      String catalogName,
      String catalogRoleName,
      String requestId,
      AuthenticatedPolarisPrincipal principal) {
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
