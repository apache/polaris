package io.polaris.core.entity;

import io.polaris.core.admin.model.CatalogRole;

/** Wrapper for translating between the REST CatalogRole object and the base PolarisEntity type. */
public class CatalogRoleEntity extends PolarisEntity {
  public CatalogRoleEntity(PolarisBaseEntity sourceEntity) {
    super(sourceEntity);
  }

  public static CatalogRoleEntity of(PolarisBaseEntity sourceEntity) {
    if (sourceEntity != null) {
      return new CatalogRoleEntity(sourceEntity);
    }
    return null;
  }

  public static CatalogRoleEntity fromCatalogRole(CatalogRole catalogRole) {
    return new Builder()
        .setName(catalogRole.getName())
        .setProperties(catalogRole.getProperties())
        .build();
  }

  public CatalogRole asCatalogRole() {
    CatalogRole catalogRole =
        new CatalogRole(
            getName(),
            getPropertiesAsMap(),
            getCreateTimestamp(),
            getLastUpdateTimestamp(),
            getEntityVersion());
    return catalogRole;
  }

  public static class Builder extends PolarisEntity.BaseBuilder<CatalogRoleEntity, Builder> {
    public Builder() {
      super();
      setType(PolarisEntityType.CATALOG_ROLE);
    }

    public Builder(CatalogRoleEntity original) {
      super(original);
    }

    public CatalogRoleEntity build() {
      return new CatalogRoleEntity(buildBase());
    }
  }
}
