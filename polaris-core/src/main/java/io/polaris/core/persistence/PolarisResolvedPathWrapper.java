package io.polaris.core.persistence;

import io.polaris.core.entity.PolarisEntity;
import java.util.List;

/**
 * Holds fully-resolved path of PolarisEntities representing the targetEntity with all its grants
 * and grant records.
 */
public class PolarisResolvedPathWrapper {
  private final List<ResolvedPolarisEntity> resolvedPath;

  // TODO: Distinguish between whether parentPath had a null in the chain or whether only
  // the leaf element was null.
  public PolarisResolvedPathWrapper(List<ResolvedPolarisEntity> resolvedPath) {
    this.resolvedPath = resolvedPath;
  }

  public ResolvedPolarisEntity getResolvedLeafEntity() {
    if (resolvedPath == null || resolvedPath.isEmpty()) {
      return null;
    }
    return resolvedPath.get(resolvedPath.size() - 1);
  }

  public PolarisEntity getRawLeafEntity() {
    ResolvedPolarisEntity resolvedEntity = getResolvedLeafEntity();
    if (resolvedEntity != null) {
      return resolvedEntity.getEntity();
    }
    return null;
  }

  public List<ResolvedPolarisEntity> getResolvedFullPath() {
    return resolvedPath;
  }

  public List<PolarisEntity> getRawFullPath() {
    if (resolvedPath == null) {
      return null;
    }
    return resolvedPath.stream().map(resolved -> resolved.getEntity()).toList();
  }

  public List<ResolvedPolarisEntity> getResolvedParentPath() {
    if (resolvedPath == null) {
      return null;
    }
    return resolvedPath.subList(0, resolvedPath.size() - 1);
  }

  public List<PolarisEntity> getRawParentPath() {
    if (resolvedPath == null) {
      return null;
    }
    return getResolvedParentPath().stream().map(resolved -> resolved.getEntity()).toList();
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("resolvedPath:");
    sb.append(resolvedPath);
    return sb.toString();
  }
}
