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
package org.apache.polaris.core.persistence;

import jakarta.annotation.Nonnull;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PolarisGrantRecord;

public class ResolvedPolarisEntity {
  private final PolarisEntity entity;

  // only non-empty if this entity can be a grantee; these are the grants on other
  // roles/securables granted to this entity.
  private final List<PolarisGrantRecord> grantRecordsAsGrantee;

  // grants associated to this entity as the securable; for a principal role or catalog role
  // these may be ROLE_USAGE or other permission-management privileges. For a catalog securable,
  // these are the grants like TABLE_READ_PROPERTIES, NAMESPACE_LIST, etc.
  private final List<PolarisGrantRecord> grantRecordsAsSecurable;

  /**
   * Constructor used when an entry is initially created after loading the entity and its grants
   * from the backend.
   *
   * @param diagnostics diagnostic services
   * @param entity the entity which has just been loaded
   * @param grantRecords associated grant records, including grants for this entity as a securable
   *     as well as grants for this entity as a grantee if applicable
   * @param grantsVersion version of the grants when they were loaded
   */
  public ResolvedPolarisEntity(
      @Nonnull PolarisDiagnostics diagnostics,
      @Nonnull PolarisBaseEntity entity,
      @Nonnull List<PolarisGrantRecord> grantRecords,
      int grantsVersion) {
    // validate not null
    diagnostics.checkNotNull(entity, "entity_null");
    diagnostics.checkNotNull(grantRecords, "grant_records_null");

    // if only the grant records have been reloaded because they were changed, the entity will
    // have an old version for those. Patch the entity if this is the case, as if we had reloaded it
    if (entity.getGrantRecordsVersion() != grantsVersion) {
      // remember the grants versions. For now grants should be loaded after the entity, so expect
      // grants version to be same or higher
      diagnostics.check(
          entity.getGrantRecordsVersion() <= grantsVersion,
          "grants_version_going_backward",
          "entity={} grantsVersion={}",
          entity,
          grantsVersion);
      // patch grant records version
      this.entity = PolarisEntity.of(entity.withGrantRecordsVersion(grantsVersion));
    } else {
      // we copy all attributes of the entity to avoid any contamination
      this.entity = PolarisEntity.of(entity);
    }

    // Split the combined list of grant records into grantee vs securable grants since the main
    // usage pattern is to get the two lists separately.
    this.grantRecordsAsGrantee =
        grantRecords.stream()
            .filter(record -> record.getGranteeId() == entity.getId())
            .collect(Collectors.toList());
    this.grantRecordsAsSecurable =
        grantRecords.stream()
            .filter(record -> record.getSecurableId() == entity.getId())
            .collect(Collectors.toList());
  }

  public ResolvedPolarisEntity(
      PolarisEntity entity,
      List<PolarisGrantRecord> grantRecordsAsGrantee,
      List<PolarisGrantRecord> grantRecordsAsSecurable) {
    this.entity = entity;
    // TODO: Precondition checks that grantee or securable ids in grant records match entity as
    // expected.
    this.grantRecordsAsGrantee = grantRecordsAsGrantee;
    this.grantRecordsAsSecurable = grantRecordsAsSecurable;
  }

  public PolarisEntity getEntity() {
    return entity;
  }

  public @Nonnull List<PolarisGrantRecord> getAllGrantRecords() {
    return Stream.concat(grantRecordsAsGrantee.stream(), grantRecordsAsSecurable.stream())
        .collect(Collectors.toList());
  }

  /** The grant records associated with this entity being the grantee of the record. */
  public List<PolarisGrantRecord> getGrantRecordsAsGrantee() {
    return grantRecordsAsGrantee;
  }

  /** The grant records associated with this entity being the securable of the record. */
  public List<PolarisGrantRecord> getGrantRecordsAsSecurable() {
    return grantRecordsAsSecurable;
  }

  @Override
  public String toString() {
    return "entity:"
        + entity
        + ";grantRecordsAsGrantee:"
        + grantRecordsAsGrantee
        + ";grantRecordsAsSecurable:"
        + grantRecordsAsSecurable;
  }
}
