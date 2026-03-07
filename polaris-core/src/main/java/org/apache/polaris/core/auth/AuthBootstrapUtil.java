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

package org.apache.polaris.core.auth;

import static com.google.common.base.Preconditions.checkState;

import com.google.common.base.Preconditions;
import java.util.Optional;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntityConstants;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.PolarisPrivilege;
import org.apache.polaris.core.entity.PrincipalEntity;
import org.apache.polaris.core.entity.PrincipalRoleEntity;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.core.persistence.dao.entity.CreatePrincipalResult;
import org.apache.polaris.core.persistence.dao.entity.GenerateEntityIdResult;
import org.apache.polaris.core.persistence.dao.entity.PrincipalSecretsResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class for sharing root Polaris Principal setup code among all persistence
 * implementations.
 *
 * <p>Note: this class is not meant to be reused outside of Polaris code.
 */
public class AuthBootstrapUtil {
  private static final Logger LOGGER = LoggerFactory.getLogger(AuthBootstrapUtil.class);

  private AuthBootstrapUtil() {}

  public static PrincipalSecretsResult createPolarisPrincipalForRealm(
      PolarisMetaStoreManager metaStoreManager, PolarisCallContext ctx) {

    Optional<PrincipalEntity> preliminaryRootPrincipal = metaStoreManager.findRootPrincipal(ctx);
    if (preliminaryRootPrincipal.isPresent()) {
      String overrideMessage =
          "It appears this metastore manager has already been bootstrapped. "
              + "To continue bootstrapping, please first purge the metastore with the `purge` command.";
      LOGGER.error("\n\n {} \n\n", overrideMessage);
      throw new IllegalArgumentException(overrideMessage);
    }

    // Create a root container entity that can represent the securable for any top-level grants.
    PolarisBaseEntity rootContainer =
        new PolarisBaseEntity(
            PolarisEntityConstants.getNullId(),
            PolarisEntityConstants.getRootEntityId(),
            PolarisEntityType.ROOT,
            PolarisEntitySubType.NULL_SUBTYPE,
            PolarisEntityConstants.getRootEntityId(),
            PolarisEntityConstants.getRootContainerName());
    metaStoreManager.createEntityIfNotExists(ctx, null, rootContainer);

    CreatePrincipalResult principalResult =
        metaStoreManager.createPrincipal(
            ctx,
            new PrincipalEntity.Builder()
                .setId(generateId(metaStoreManager, ctx))
                .setName(PolarisEntityConstants.getRootPrincipalName())
                .setCreateTimestamp(System.currentTimeMillis())
                .build());
    checkState(principalResult.isSuccess(), "Unable to create root principal");
    PrincipalEntity rootPrincipal = principalResult.getPrincipal();

    // now create the account admin principal role
    PrincipalRoleEntity serviceAdminPrincipalRole =
        new PrincipalRoleEntity.Builder()
            .setId(generateId(metaStoreManager, ctx))
            .setName(PolarisEntityConstants.getNameOfPrincipalServiceAdminRole())
            .setCreateTimestamp(System.currentTimeMillis())
            .build();
    metaStoreManager.createEntityIfNotExists(ctx, null, serviceAdminPrincipalRole);

    // we also need to grant usage on the account-admin principal to the principal
    metaStoreManager.grantPrivilegeOnSecurableToRole(
        ctx, rootPrincipal, null, serviceAdminPrincipalRole, PolarisPrivilege.PRINCIPAL_ROLE_USAGE);

    // grant SERVICE_MANAGE_ACCESS on the rootContainer to the serviceAdminPrincipalRole
    metaStoreManager.grantPrivilegeOnSecurableToRole(
        ctx,
        serviceAdminPrincipalRole,
        null,
        rootContainer,
        PolarisPrivilege.SERVICE_MANAGE_ACCESS);

    // create the catalog_role_manager principal role for catalog admins to list principal roles
    PrincipalRoleEntity catalogRoleManagerPrincipalRole =
        new PrincipalRoleEntity.Builder()
            .setId(generateId(metaStoreManager, ctx))
            .setName(PolarisEntityConstants.getNameOfCatalogRoleManagerPrincipalRole())
            .setCreateTimestamp(System.currentTimeMillis())
            .build();
    metaStoreManager.createEntityIfNotExists(ctx, null, catalogRoleManagerPrincipalRole);

    // grant PRINCIPAL_ROLE_LIST on the rootContainer to the catalogRoleManagerPrincipalRole
    metaStoreManager.grantPrivilegeOnSecurableToRole(
        ctx,
        catalogRoleManagerPrincipalRole,
        null,
        rootContainer,
        PolarisPrivilege.PRINCIPAL_ROLE_LIST);

    return metaStoreManager.loadPrincipalSecrets(ctx, rootPrincipal.getClientId());
  }

  private static long generateId(PolarisMetaStoreManager metaStoreManager, PolarisCallContext ctx) {
    GenerateEntityIdResult res = metaStoreManager.generateNewEntityId(ctx);
    Preconditions.checkState(res.isSuccess(), "Unable to generate id for polaris entity");
    return res.getId();
  }
}
