package io.polaris.core.entity;

import io.polaris.core.admin.model.PrincipalRole;

/**
 * Wrapper for translating between the REST PrincipalRole object and the base PolarisEntity type.
 */
public class PrincipalRoleEntity extends PolarisEntity {
  public PrincipalRoleEntity(PolarisBaseEntity sourceEntity) {
    super(sourceEntity);
  }

  public static PrincipalRoleEntity of(PolarisBaseEntity sourceEntity) {
    if (sourceEntity != null) {
      return new PrincipalRoleEntity(sourceEntity);
    }
    return null;
  }

  public static PrincipalRoleEntity fromPrincipalRole(PrincipalRole principalRole) {
    return new Builder()
        .setName(principalRole.getName())
        .setProperties(principalRole.getProperties())
        .build();
  }

  public PrincipalRole asPrincipalRole() {
    PrincipalRole principalRole =
        new PrincipalRole(
            getName(),
            getPropertiesAsMap(),
            getCreateTimestamp(),
            getLastUpdateTimestamp(),
            getEntityVersion());
    return principalRole;
  }

  public static class Builder extends PolarisEntity.BaseBuilder<PrincipalRoleEntity, Builder> {
    public Builder() {
      super();
      setType(PolarisEntityType.PRINCIPAL_ROLE);
      setCatalogId(PolarisEntityConstants.getNullId());
      setParentId(PolarisEntityConstants.getRootEntityId());
    }

    public Builder(PrincipalRoleEntity original) {
      super(original);
    }

    public PrincipalRoleEntity build() {
      return new PrincipalRoleEntity(buildBase());
    }
  }
}
