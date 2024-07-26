package io.polaris.core.persistence.resolver;

import com.google.common.collect.ImmutableList;
import io.polaris.core.entity.PolarisEntityType;
import java.util.List;

/** Simple class to represent a path within a catalog */
public class ResolverPath {

  // name of the entities in that path. The parent of the first named entity is the path is the
  // catalog
  private final List<String> entityNames;

  // all entities in a path are namespaces except the last one which can be  a table_like entity
  // versus a namespace
  private final PolarisEntityType lastEntityType;

  // true if this path is optional, i.e. failing to fully resolve it is not an error
  private final boolean isOptional;

  /**
   * Constructor for an optional path
   *
   * @param entityNames set of entity names, all are namespaces except the last one which is either
   *     a namespace or a table_like entity
   * @param lastEntityType type of the last entity, either namespace or table_like
   */
  public ResolverPath(List<String> entityNames, PolarisEntityType lastEntityType) {
    this(entityNames, lastEntityType, false);
  }

  /**
   * Constructor for an optional path
   *
   * @param entityNames set of entity names, all are namespaces except the last one which is either
   *     a namespace or a table_like entity
   * @param lastEntityType type of the last entity, either namespace or table_like
   * @param isOptional true if optional
   */
  public ResolverPath(
      List<String> entityNames, PolarisEntityType lastEntityType, boolean isOptional) {
    this.entityNames = ImmutableList.copyOf(entityNames);
    this.lastEntityType = lastEntityType;
    this.isOptional = isOptional;
  }

  public List<String> getEntityNames() {
    return entityNames;
  }

  public PolarisEntityType getLastEntityType() {
    return lastEntityType;
  }

  public boolean isOptional() {
    return isOptional;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("entityNames:");
    sb.append(entityNames.toString());
    sb.append(";lastEntityType:");
    sb.append(lastEntityType.toString());
    sb.append(";isOptional:");
    sb.append(isOptional);
    return sb.toString();
  }
}
