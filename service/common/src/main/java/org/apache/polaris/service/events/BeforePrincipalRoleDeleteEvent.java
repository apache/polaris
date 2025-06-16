package org.apache.polaris.service.events;

import org.apache.polaris.core.auth.AuthenticatedPolarisPrincipal;

/** Event fired before a principal role is deleted in Polaris. */
public class BeforePrincipalRoleDeleteEvent implements PolarisEvent {
  private final String principalRoleName;
  private final String requestId;
  private final AuthenticatedPolarisPrincipal principal;

  /**
   * Constructs a new BeforePrincipalRoleDeleteEvent.
   *
   * @param principalRoleName the name of the principal role to be deleted
   * @param requestId the request ID for this operation
   * @param principal the authenticated principal performing the operation
   */
  public BeforePrincipalRoleDeleteEvent(
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
