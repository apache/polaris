package io.polaris.core.storage.cache;

import io.polaris.core.PolarisCallContext;
import io.polaris.core.context.CallContext;
import io.polaris.core.entity.PolarisEntity;
import io.polaris.core.entity.PolarisEntityConstants;
import java.util.Objects;
import java.util.Set;
import org.jetbrains.annotations.Nullable;

public class StorageCredentialCacheKey {

  private final long catalogId;

  /** The serialized string of the storage config. */
  private final String storageConfigSerializedStr;

  /**
   * The entity id is passed to be used to fetch subscoped creds, but is not used to do hash/equals
   * as part of the cache key.
   */
  private final long entityId;

  private final boolean allowedListAction;
  private final Set<String> allowedReadLocations;

  private final Set<String> allowedWriteLocations;

  /**
   * The callContext is passed to be used to fetch subscoped creds, but is not used to hash/equals
   * as part of the cache key.
   */
  private @Nullable PolarisCallContext callContext;

  public StorageCredentialCacheKey(
      PolarisEntity entity,
      boolean allowedListAction,
      Set<String> allowedReadLocations,
      Set<String> allowedWriteLocations,
      PolarisCallContext callContext) {
    this.catalogId = entity.getCatalogId();
    this.storageConfigSerializedStr =
        entity
            .getInternalPropertiesAsMap()
            .get(PolarisEntityConstants.getStorageConfigInfoPropertyName());
    this.entityId = entity.getId();
    this.allowedListAction = allowedListAction;
    this.allowedReadLocations = allowedReadLocations;
    this.allowedWriteLocations = allowedWriteLocations;
    this.callContext = callContext;
    if (this.callContext == null) {
      this.callContext = CallContext.getCurrentContext().getPolarisCallContext();
    }
  }

  public long getCatalogId() {
    return catalogId;
  }

  public String getStorageConfigSerializedStr() {
    return storageConfigSerializedStr;
  }

  public long getEntityId() {
    return entityId;
  }

  public boolean isAllowedListAction() {
    return allowedListAction;
  }

  public Set<String> getAllowedReadLocations() {
    return allowedReadLocations;
  }

  public Set<String> getAllowedWriteLocations() {
    return allowedWriteLocations;
  }

  public PolarisCallContext getCallContext() {
    return callContext;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    StorageCredentialCacheKey cacheKey = (StorageCredentialCacheKey) o;
    return catalogId == cacheKey.getCatalogId()
        && Objects.equals(storageConfigSerializedStr, cacheKey.getStorageConfigSerializedStr())
        && allowedListAction == cacheKey.allowedListAction
        && Objects.equals(allowedReadLocations, cacheKey.allowedReadLocations)
        && Objects.equals(allowedWriteLocations, cacheKey.allowedWriteLocations);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        catalogId,
        storageConfigSerializedStr,
        allowedListAction,
        allowedReadLocations,
        allowedWriteLocations);
  }

  @Override
  public String toString() {
    return "StorageCredentialCacheKey{"
        + "catalogId="
        + catalogId
        + ", storageConfigSerializedStr='"
        + storageConfigSerializedStr
        + '\''
        + ", entityId="
        + entityId
        + ", allowedListAction="
        + allowedListAction
        + ", allowedReadLocations="
        + allowedReadLocations
        + ", allowedWriteLocations="
        + allowedWriteLocations
        + '}';
  }
}
