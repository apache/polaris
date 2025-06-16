package org.apache.polaris.service.events;

import org.apache.polaris.core.auth.AuthenticatedPolarisPrincipal;

/** Event fired before a principal role is retrieved in Polaris. */
public class BeforePrincipalRoleGetEvent implements PolarisEvent {
  private final String principalRoleName;
  private final String requestId;
  private final AuthenticatedPolarisPrincipal principal;

  /**
   * Constructs a new BeforePrincipalRoleGetEvent.
   *
   * @param principalRoleName the name of the principal role to be retrieved
   * @param requestId the request ID for this operation
   * @param principal the authenticated principal performing the operation
   */
  public BeforePrincipalRoleGetEvent(
      String principalRoleName, String requestId, AuthenticatedPolarisPrincipal principal) {
    this.principalRoleName = principalRoleName;
    this.requestId = requestId;
    this.principal = principal;
  }

  public String getPrincipalRoleName() {
    return principalRoleName;
  }

  public String getRequestId() {
    return requestId;
  }

  public AuthenticatedPolarisPrincipal getPrincipal() {
    return principal;
  }
}
