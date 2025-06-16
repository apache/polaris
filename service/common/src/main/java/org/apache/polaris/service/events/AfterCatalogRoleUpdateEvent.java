package org.apache.polaris.service.events;

import org.apache.polaris.core.admin.model.CatalogRole;
import org.apache.polaris.core.auth.AuthenticatedPolarisPrincipal;

/** Event fired after a catalog role is updated in Polaris. */
public class AfterCatalogRoleUpdateEvent implements PolarisEvent {
  private final String catalogName;
  private final CatalogRole updatedCatalogRole;
  private final String requestId;
  private final AuthenticatedPolarisPrincipal principal;

  /**
   * Constructs a new AfterCatalogRoleUpdateEvent.
   *
   * @param catalogName the name of the catalog
   * @param updatedCatalogRole the updated catalog role object
   * @param requestId the request ID for this operation
   * @param principal the authenticated principal performing the operation
   */
  public AfterCatalogRoleUpdateEvent(
      String catalogName,
      CatalogRole updatedCatalogRole,
      String requestId,
      AuthenticatedPolarisPrincipal principal) {
    this.catalogName = catalogName;
    this.updatedCatalogRole = updatedCatalogRole;
    this.requestId = requestId;
    this.principal = principal;
  }

  public String getCatalogName() {
    return catalogName;
  }

  public CatalogRole getUpdatedCatalogRole() {
    return updatedCatalogRole;
  }

  public String getRequestId() {
    return requestId;
  }

  public AuthenticatedPolarisPrincipal getPrincipal() {
    return principal;
  }
}
