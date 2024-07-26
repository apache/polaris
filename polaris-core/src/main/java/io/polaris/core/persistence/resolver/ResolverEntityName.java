package io.polaris.core.persistence.resolver;

import io.polaris.core.entity.PolarisEntityType;
import java.util.Objects;

/** Simple class to represent the name of an entity to resolve */
public class ResolverEntityName {

  // type of the entity
  private final PolarisEntityType entityType;

  // the name of the entity
  private final String entityName;

  // true if we should not fail while resolving this entity
  private final boolean isOptional;

  public ResolverEntityName(PolarisEntityType entityType, String entityName, boolean isOptional) {
    this.entityType = entityType;
    this.entityName = entityName;
    this.isOptional = isOptional;
  }

  public PolarisEntityType getEntityType() {
    return entityType;
  }

  public String getEntityName() {
    return entityName;
  }

  public boolean isOptional() {
    return isOptional;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ResolverEntityName that = (ResolverEntityName) o;
    return getEntityType() == that.getEntityType()
        && Objects.equals(getEntityName(), that.getEntityName());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getEntityType(), getEntityName());
  }
}
