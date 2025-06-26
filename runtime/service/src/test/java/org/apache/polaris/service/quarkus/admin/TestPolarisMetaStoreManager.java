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

package org.apache.polaris.service.quarkus.admin;

import jakarta.annotation.Nonnull;
import java.util.HashSet;
import java.util.Set;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.persistence.dao.entity.BaseResult;
import org.apache.polaris.core.persistence.dao.entity.EntityResult;
import org.apache.polaris.core.persistence.transactional.TransactionalMetaStoreManagerImpl;

/**
 * Intended to be a delegate to TransactionalMetaStoreManagerImpl with the ability to inject faults.
 * Currently, you can force loadEntity() to return ENTITY_NOT_FOUND for a set of entity IDs.
 */
public class TestPolarisMetaStoreManager extends TransactionalMetaStoreManagerImpl {
  public Set<Long> fakeEntityNotFoundIds = new HashSet<>();

  @Override
  public @Nonnull EntityResult loadEntity(
      @Nonnull PolarisCallContext callCtx,
      long entityCatalogId,
      long entityId,
      @Nonnull PolarisEntityType entityType) {
    if (fakeEntityNotFoundIds.contains(entityId)) {
      return new EntityResult(BaseResult.ReturnStatus.ENTITY_NOT_FOUND, "");
    }
    return super.loadEntity(callCtx, entityCatalogId, entityId, entityType);
  }
}
