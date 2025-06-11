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

import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.entity.*;
import org.apache.polaris.core.persistence.BasePersistence;

/**
 * Fake implementation of {@link BasePersistence} for testing purposes. This implementation does
 * nothing. Any actual use of this class will likely throw a {@link NullPointerException} as all
 * methods return null.
 */
public class FakeBasePersistence implements BasePersistence {
  @Override
  public long generateNewId(PolarisCallContext callCtx) {
    return 0;
  }

  @Override
  public void writeEntity(
      PolarisCallContext callCtx,
      PolarisBaseEntity entity,
      boolean nameOrParentChanged,
      PolarisBaseEntity originalEntity) {}

  @Override
  public void writeEntities(
      PolarisCallContext callCtx,
      List<PolarisBaseEntity> entities,
      List<PolarisBaseEntity> originalEntities) {}

  @Override
  public void writeToGrantRecords(PolarisCallContext callCtx, PolarisGrantRecord grantRec) {}

  @Override
  public void deleteEntity(PolarisCallContext callCtx, PolarisBaseEntity entity) {}

  @Override
  public void deleteFromGrantRecords(PolarisCallContext callCtx, PolarisGrantRecord grantRec) {}

  @Override
  public void deleteAllEntityGrantRecords(
      PolarisCallContext callCtx,
      PolarisEntityCore entity,
      List<PolarisGrantRecord> grantsOnGrantee,
      List<PolarisGrantRecord> grantsOnSecurable) {}

  @Override
  public void deleteAll(PolarisCallContext callCtx) {}

  @Override
  public PolarisBaseEntity lookupEntity(
      PolarisCallContext callCtx, long catalogId, long entityId, int typeCode) {
    return null;
  }

  @Override
  public PolarisBaseEntity lookupEntityByName(
      PolarisCallContext callCtx, long catalogId, long parentId, int typeCode, String name) {
    return null;
  }

  @Override
  public List<PolarisBaseEntity> lookupEntities(
      PolarisCallContext callCtx, List<PolarisEntityId> entityIds) {
    return List.of();
  }

  @Override
  public List<PolarisChangeTrackingVersions> lookupEntityVersions(
      PolarisCallContext callCtx, List<PolarisEntityId> entityIds) {
    return List.of();
  }

  @Override
  public List<EntityNameLookupRecord> listEntities(
      PolarisCallContext callCtx, long catalogId, long parentId, PolarisEntityType entityType) {
    return List.of();
  }

  @Override
  public List<EntityNameLookupRecord> listEntities(
      PolarisCallContext callCtx,
      long catalogId,
      long parentId,
      PolarisEntityType entityType,
      Predicate<PolarisBaseEntity> entityFilter) {
    return List.of();
  }

  @Override
  public <T> List<T> listEntities(
      PolarisCallContext callCtx,
      long catalogId,
      long parentId,
      PolarisEntityType entityType,
      int limit,
      Predicate<PolarisBaseEntity> entityFilter,
      Function<PolarisBaseEntity, T> transformer) {
    return List.of();
  }

  @Override
  public int lookupEntityGrantRecordsVersion(
      PolarisCallContext callCtx, long catalogId, long entityId) {
    return 0;
  }

  @Override
  public PolarisGrantRecord lookupGrantRecord(
      PolarisCallContext callCtx,
      long securableCatalogId,
      long securableId,
      long granteeCatalogId,
      long granteeId,
      int privilegeCode) {
    return null;
  }

  @Override
  public List<PolarisGrantRecord> loadAllGrantRecordsOnSecurable(
      PolarisCallContext callCtx, long securableCatalogId, long securableId) {
    return List.of();
  }

  @Override
  public List<PolarisGrantRecord> loadAllGrantRecordsOnGrantee(
      PolarisCallContext callCtx, long granteeCatalogId, long granteeId) {
    return List.of();
  }

  @Override
  public boolean hasChildren(
      PolarisCallContext callContext,
      PolarisEntityType optionalEntityType,
      long catalogId,
      long parentId) {
    return false;
  }
}
