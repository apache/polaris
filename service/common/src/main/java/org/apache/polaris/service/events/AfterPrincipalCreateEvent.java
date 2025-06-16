package org.apache.polaris.service.events;

import org.apache.polaris.core.auth.AuthenticatedPolarisPrincipal;

/** Emitted after a principal is created in Polaris. */
public class AfterPrincipalCreateEvent implements PolarisEvent {
  private final String principalName;
  private final String user;

  public AfterPrincipalCreateEvent(String principalName, AuthenticatedPolarisPrincipal principal) {
    this.principalName = principalName;
    if (principal != null) {
      this.user = principal.getName();
    } else {
      this.user = null;
    }
  }

  public String getPrincipalName() {
    return principalName;
  }

  public String getUser() {
    return user;
  }
}
