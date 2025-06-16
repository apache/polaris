package org.apache.polaris.service.events;

import org.apache.polaris.core.auth.AuthenticatedPolarisPrincipal;

/** Event fired before a catalog role is created in Polaris. */
public class BeforeCatalogRoleCreateEvent implements PolarisEvent {
  private final String catalogName;
  private final String catalogRoleName;
  private final String requestId;
  private final AuthenticatedPolarisPrincipal principal;

  /**
   * Constructs a new BeforeCatalogRoleCreateEvent.
   *
   * @param catalogName the name of the catalog
   * @param catalogRoleName the name of the catalog role to be created
   * @param requestId the request ID for this operation
   * @param principal the authenticated principal performing the operation
   */
  public BeforeCatalogRoleCreateEvent(
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
