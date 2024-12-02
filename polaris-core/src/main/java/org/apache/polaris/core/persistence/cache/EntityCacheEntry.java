/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.polaris.core.persistence.cache;

import com.google.common.collect.ImmutableList;
import jakarta.annotation.Nonnull;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisGrantRecord;

/** An entry in our entity cache. Note, this is fully immutable */
public class EntityCacheEntry {

  // epoch time (ns) when the cache entry was added to the cache
  private final long createdOnNanoTimestamp;

  // epoch time (ns) when the cache entry was added to the cache
  private long lastAccessedNanoTimestamp;

  // the entity which have been cached.
  private final PolarisBaseEntity entity;

  // grants associated to this entity, for a principal, a principal role, or a catalog role these
  // are role usage
  // grants on that entity. For a catalog securable (i.e. a catalog, namespace, or table_like
  // securable), these are
  // the grants on this securable.
  private final List<PolarisGrantRecord> grantRecords;

  /**
   * Constructor used when an entry is initially created after loading the entity and its grants
   * from the backend.
   *
   * @param diagnostics diagnostic services
   * @param createdOnNanoTimestamp when the entity was created
   * @param entity the entity which has just been loaded
   * @param grantRecords associated grant records, including grants for this entity as a securable
   *     as well as grants for this entity as a grantee if applicable
   * @param grantsVersion version of the grants when they were loaded
   */
  EntityCacheEntry(
      @Nonnull PolarisDiagnostics diagnostics,
      long createdOnNanoTimestamp,
      @Nonnull PolarisBaseEntity entity,
      @Nonnull List<PolarisGrantRecord> grantRecords,
      int grantsVersion) {
    // validate not null
    diagnostics.checkNotNull(entity, "entity_null");
    diagnostics.checkNotNull(grantRecords, "grant_records_null");

    // when this entry has been created
    this.createdOnNanoTimestamp = createdOnNanoTimestamp;

    // last accessed time is now
    this.lastAccessedNanoTimestamp = System.nanoTime();

    // we copy all attributes of the entity to avoid any contamination
    this.entity = new PolarisBaseEntity(entity);

    // if only the grant records have been reloaded because they were changed, the entity will
    // have an old version for those. Patch the entity if this is the case, as if we had reloaded it
    if (this.entity.getGrantRecordsVersion() != grantsVersion) {
      // remember the grants versions. For now grants should be loaded after the entity, so expect
      // grants version to be same or higher
      diagnostics.check(
          this.entity.getGrantRecordsVersion() <= grantsVersion,
          "grants_version_going_backward",
          "entity={} grantsVersion={}",
          entity,
          grantsVersion);

      // patch grant records version
      this.entity.setGrantRecordsVersion(grantsVersion);
    }

    // the grants
    this.grantRecords = ImmutableList.copyOf(grantRecords);
  }

  public long getCreatedOnNanoTimestamp() {
    return createdOnNanoTimestamp;
  }

  public long getLastAccessedNanoTimestamp() {
    return lastAccessedNanoTimestamp;
  }

  public @Nonnull PolarisBaseEntity getEntity() {
    return entity;
  }

  public @Nonnull List<PolarisGrantRecord> getAllGrantRecords() {
    return grantRecords;
  }

  public @Nonnull List<PolarisGrantRecord> getGrantRecordsAsGrantee() {
    return grantRecords.stream()
        .filter(record -> record.getGranteeId() == entity.getId())
        .collect(Collectors.toList());
  }

  public @Nonnull List<PolarisGrantRecord> getGrantRecordsAsSecurable() {
    return grantRecords.stream()
        .filter(record -> record.getSecurableId() == entity.getId())
        .collect(Collectors.toList());
  }

  public void updateLastAccess() {
    this.lastAccessedNanoTimestamp = System.nanoTime();
  }
}
