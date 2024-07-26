package io.polaris.core.persistence.resolver;

import io.polaris.core.entity.PolarisEntitySubType;
import io.polaris.core.persistence.PolarisResolvedPathWrapper;

/**
 * Defines the methods by which a Catalog is expected to access resolved catalog-path entities,
 * typically backed by a PolarisResolutionManifest.
 */
public interface PolarisResolutionManifestCatalogView {
  PolarisResolvedPathWrapper getResolvedReferenceCatalogEntity();

  PolarisResolvedPathWrapper getResolvedPath(Object key);

  PolarisResolvedPathWrapper getResolvedPath(Object key, PolarisEntitySubType subType);

  PolarisResolvedPathWrapper getPassthroughResolvedPath(Object key);

  PolarisResolvedPathWrapper getPassthroughResolvedPath(Object key, PolarisEntitySubType subType);
}
