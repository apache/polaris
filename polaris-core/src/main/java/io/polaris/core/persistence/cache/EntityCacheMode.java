package io.polaris.core.persistence.cache;

/** Cache mode, the default is ENABLE. */
public enum EntityCacheMode {
  // bypass the cache, always load
  BYPASS,
  // enable the cache, this is the default
  ENABLE,
  // enable but verify that the cache content is consistent. Used in QA mode to detect when
  // versioning information is
  // not properly maintained
  ENABLE_BUT_VERIFY
}
