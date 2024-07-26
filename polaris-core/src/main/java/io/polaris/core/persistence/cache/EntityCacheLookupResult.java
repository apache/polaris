package io.polaris.core.persistence.cache;

import org.jetbrains.annotations.Nullable;

/** Result of a lookup operation */
public class EntityCacheLookupResult {

  // if not null, we found the entity and this is the entry. If not found, the entity was dropped or
  // does not exist
  private final @Nullable EntityCacheEntry cacheEntry;

  // true if the entity was found in the cache
  private final boolean cacheHit;

  public EntityCacheLookupResult(@Nullable EntityCacheEntry cacheEntry, boolean cacheHit) {
    this.cacheEntry = cacheEntry;
    this.cacheHit = cacheHit;
  }

  public @Nullable EntityCacheEntry getCacheEntry() {
    return cacheEntry;
  }

  public boolean isCacheHit() {
    return cacheHit;
  }
}
