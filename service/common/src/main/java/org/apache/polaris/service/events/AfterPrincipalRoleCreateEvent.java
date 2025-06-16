package org.apache.polaris.service.events;

import org.apache.polaris.core.admin.model.PrincipalRole;
import org.apache.polaris.core.auth.AuthenticatedPolarisPrincipal;

/** Event fired after a principal role is created in Polaris. */
public class AfterPrincipalRoleCreateEvent implements PolarisEvent {
  private final PrincipalRole principalRole;
  private final String requestId;
  private final AuthenticatedPolarisPrincipal principal;

  /**
   * Constructs a new AfterPrincipalRoleCreateEvent.
   *
   * @param principalRole the new Principal Role entity created
   * @param requestId the request ID for this operation
   * @param principal the authenticated principal performing the operation
   */
  public AfterPrincipalRoleCreateEvent(
      PrincipalRole principalRole, String requestId, AuthenticatedPolarisPrincipal principal) {
    this.principalRole = principalRole;
    this.requestId = requestId;
    this.principal = principal;
  }

  public PrincipalRole getPrincipalRole() {
    return principalRole;
  }

  public String getRequestId() {
    return requestId;
  }

  public AuthenticatedPolarisPrincipal getPrincipal() {
    return principal;
  }
}
