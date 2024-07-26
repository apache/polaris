package io.polaris.core.auth;

import io.polaris.core.entity.PolarisEntity;
import io.polaris.core.entity.PrincipalRoleEntity;
import java.util.List;
import java.util.Set;
import org.jetbrains.annotations.NotNull;

/** Holds the results of request authentication. */
public class AuthenticatedPolarisPrincipal implements java.security.Principal {
  private final PolarisEntity principalEntity;
  private final Set<String> activatedPrincipalRoleNames;
  // only known and set after the above set of principal role names have been resolved. Before
  // this, this list is null
  private List<PrincipalRoleEntity> activatedPrincipalRoles;

  public AuthenticatedPolarisPrincipal(
      @NotNull PolarisEntity principalEntity, @NotNull Set<String> activatedPrincipalRoles) {
    this.principalEntity = principalEntity;
    this.activatedPrincipalRoleNames = activatedPrincipalRoles;
    this.activatedPrincipalRoles = null;
  }

  @Override
  public String getName() {
    return principalEntity.getName();
  }

  public PolarisEntity getPrincipalEntity() {
    return principalEntity;
  }

  public Set<String> getActivatedPrincipalRoleNames() {
    return activatedPrincipalRoleNames;
  }

  public List<PrincipalRoleEntity> getActivatedPrincipalRoles() {
    return activatedPrincipalRoles;
  }

  public void setActivatedPrincipalRoles(List<PrincipalRoleEntity> activatedPrincipalRoles) {
    this.activatedPrincipalRoles = activatedPrincipalRoles;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("principalEntity=" + getPrincipalEntity());
    sb.append(";activatedPrincipalRoleNames=" + getActivatedPrincipalRoleNames());
    sb.append(";activatedPrincipalRoles=" + getActivatedPrincipalRoles());
    return sb.toString();
  }
}
