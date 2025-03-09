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
package org.apache.polaris.extension.persistence.impl.eclipselink.postgres;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.List;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.auth.PolarisGrantManager;
import org.apache.polaris.core.entity.PolarisEntityCore;
import org.apache.polaris.core.entity.PolarisPrivilege;
import org.apache.polaris.core.persistence.dao.GrantRecordDao;
import org.apache.polaris.core.persistence.transactional.PolarisMetaStoreManagerImpl;

public class PostgresGrantRecordDaoImpl implements GrantRecordDao {
  PolarisMetaStoreManagerImpl metaStoreManager = new PolarisMetaStoreManagerImpl();

  @Nonnull
  @Override
  public PolarisGrantManager.PrivilegeResult grantUsageOnRoleToGrantee(
      @Nonnull PolarisCallContext callCtx,
      @Nullable PolarisEntityCore catalog,
      @Nonnull PolarisEntityCore role,
      @Nonnull PolarisEntityCore grantee) {
    return metaStoreManager.grantUsageOnRoleToGrantee(callCtx, catalog, role, grantee);
  }

  @Nonnull
  @Override
  public PolarisGrantManager.PrivilegeResult revokeUsageOnRoleFromGrantee(
      @Nonnull PolarisCallContext callCtx,
      @Nullable PolarisEntityCore catalog,
      @Nonnull PolarisEntityCore role,
      @Nonnull PolarisEntityCore grantee) {
    return metaStoreManager.revokeUsageOnRoleFromGrantee(callCtx, catalog, role, grantee);
  }

  @Nonnull
  @Override
  public PolarisGrantManager.PrivilegeResult grantPrivilegeOnSecurableToRole(
      @Nonnull PolarisCallContext callCtx,
      @Nonnull PolarisEntityCore grantee,
      @Nullable List<PolarisEntityCore> catalogPath,
      @Nonnull PolarisEntityCore securable,
      @Nonnull PolarisPrivilege privilege) {
    return metaStoreManager.grantPrivilegeOnSecurableToRole(
        callCtx, grantee, catalogPath, securable, privilege);
  }

  @Nonnull
  @Override
  public PolarisGrantManager.PrivilegeResult revokePrivilegeOnSecurableFromRole(
      @Nonnull PolarisCallContext callCtx,
      @Nonnull PolarisEntityCore grantee,
      @Nullable List<PolarisEntityCore> catalogPath,
      @Nonnull PolarisEntityCore securable,
      @Nonnull PolarisPrivilege privilege) {
    return metaStoreManager.revokePrivilegeOnSecurableFromRole(
        callCtx, grantee, catalogPath, securable, privilege);
  }

  @Nonnull
  @Override
  public PolarisGrantManager.LoadGrantsResult loadGrantsOnSecurable(
      @Nonnull PolarisCallContext callCtx, long securableCatalogId, long securableId) {
    return metaStoreManager.loadGrantsOnSecurable(callCtx, securableCatalogId, securableId);
  }

  @Nonnull
  @Override
  public PolarisGrantManager.LoadGrantsResult loadGrantsToGrantee(
      PolarisCallContext callCtx, long granteeCatalogId, long granteeId) {
    return metaStoreManager.loadGrantsToGrantee(callCtx, granteeCatalogId, granteeId);
  }
}
