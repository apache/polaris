package io.polaris.core.entity;

import io.polaris.core.admin.model.Principal;

/** Wrapper for translating between the REST Principal object and the base PolarisEntity type. */
public class PrincipalEntity extends PolarisEntity {
  public PrincipalEntity(PolarisBaseEntity sourceEntity) {
    super(sourceEntity);
  }

  public static PrincipalEntity of(PolarisBaseEntity sourceEntity) {
    if (sourceEntity != null) {
      return new PrincipalEntity(sourceEntity);
    }
    return null;
  }

  public static PrincipalEntity fromPrincipal(Principal principal) {
    return new Builder()
        .setName(principal.getName())
        .setProperties(principal.getProperties())
        .setClientId(principal.getClientId())
        .build();
  }

  public Principal asPrincipal() {
    return new Principal(
        getName(),
        getClientId(),
        getPropertiesAsMap(),
        getCreateTimestamp(),
        getLastUpdateTimestamp(),
        getEntityVersion());
  }

  public String getClientId() {
    return getInternalPropertiesAsMap().get(PolarisEntityConstants.getClientIdPropertyName());
  }

  public static class Builder extends PolarisEntity.BaseBuilder<PrincipalEntity, Builder> {
    public Builder() {
      super();
      setType(PolarisEntityType.PRINCIPAL);
      setCatalogId(PolarisEntityConstants.getNullId());
      setParentId(PolarisEntityConstants.getRootEntityId());
    }

    public Builder(PrincipalEntity original) {
      super(original);
    }

    public Builder setClientId(String clientId) {
      internalProperties.put(PolarisEntityConstants.getClientIdPropertyName(), clientId);
      return this;
    }

    public Builder setCredentialRotationRequiredState() {
      internalProperties.put(
          PolarisEntityConstants.PRINCIPAL_CREDENTIAL_ROTATION_REQUIRED_STATE, "true");
      return this;
    }

    public PrincipalEntity build() {
      return new PrincipalEntity(buildBase());
    }
  }
}
