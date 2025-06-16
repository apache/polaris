package org.apache.polaris.service.events;

import org.apache.polaris.core.admin.model.UpdatePrincipalRequest;
import org.apache.polaris.core.auth.AuthenticatedPolarisPrincipal;

/** Event fired before a principal is updated in Polaris. */
public class BeforePrincipalUpdateEvent implements PolarisEvent {
  private final String principalName;
  private final UpdatePrincipalRequest updateRequest;
  private final String requestId;
  private final AuthenticatedPolarisPrincipal principal;

  /**
   * Constructs a new BeforePrincipalUpdateEvent.
   *
   * @param principalName the name of the principal to be updated
   * @param updateRequest the update request object
   * @param requestId the request ID for this operation
   * @param principal the authenticated principal performing the operation
   */
  public BeforePrincipalUpdateEvent(
      String principalName,
      UpdatePrincipalRequest updateRequest,
      String requestId,
      AuthenticatedPolarisPrincipal principal) {
    this.principalName = principalName;
    this.updateRequest = updateRequest;
    this.requestId = requestId;
    this.principal = principal;
  }

  public String getPrincipalName() {
    return principalName;
  }

  public UpdatePrincipalRequest getUpdateRequest() {
    return updateRequest;
  }

  public String getRequestId() {
    return requestId;
  }

  public AuthenticatedPolarisPrincipal getPrincipal() {
    return principal;
  }
}
