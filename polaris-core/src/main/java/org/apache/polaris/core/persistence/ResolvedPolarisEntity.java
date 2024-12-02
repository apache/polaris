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

import com.google.common.collect.ImmutableList;
import jakarta.annotation.Nonnull;
import java.util.List;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PolarisGrantRecord;
import org.apache.polaris.core.persistence.cache.EntityCacheEntry;

public class ResolvedPolarisEntity {
  private final PolarisEntity entity;

  // only non-empty if this entity can be a grantee; these are the grants on other
  // roles/securables granted to this entity.
  private final List<PolarisGrantRecord> grantRecordsAsGrantee;

  // grants associated to this entity as the securable; for a principal role or catalog role
  // these may be ROLE_USAGE or other permission-management privileges. For a catalog securable,
  // these are the grants like TABLE_READ_PROPERTIES, NAMESPACE_LIST, etc.
  private final List<PolarisGrantRecord> grantRecordsAsSecurable;

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

  public ResolvedPolarisEntity(@Nonnull EntityCacheEntry cacheEntry) {
    this.entity = PolarisEntity.of(cacheEntry.getEntity());
    this.grantRecordsAsGrantee = ImmutableList.copyOf(cacheEntry.getGrantRecordsAsGrantee());
    this.grantRecordsAsSecurable = ImmutableList.copyOf(cacheEntry.getGrantRecordsAsSecurable());
  }

  public PolarisEntity getEntity() {
    return entity;
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
