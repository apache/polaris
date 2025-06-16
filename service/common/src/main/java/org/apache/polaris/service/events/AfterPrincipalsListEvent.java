package org.apache.polaris.service.events;

import org.apache.polaris.core.auth.AuthenticatedPolarisPrincipal;

/** Event fired after the list of principals is retrieved in Polaris. */
public class AfterPrincipalsListEvent implements PolarisEvent {
  private final String requestId;
  private final AuthenticatedPolarisPrincipal principal;

  /**
   * Constructs a new AfterPrincipalsListEvent.
   *
   * @param requestId the request ID for this operation
   * @param principal the authenticated principal performing the operation
   */
  public AfterPrincipalsListEvent(String requestId, AuthenticatedPolarisPrincipal principal) {
    this.requestId = requestId;
    this.principal = principal;
  }

  public String getRequestId() {
    return requestId;
  }

  public AuthenticatedPolarisPrincipal getPrincipal() {
    return principal;
  }
}
