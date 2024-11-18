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

import static org.apache.polaris.core.entity.PolarisPrivilege.CATALOG_CREATE;
import static org.apache.polaris.core.entity.PolarisPrivilege.CATALOG_DROP;
import static org.apache.polaris.core.entity.PolarisPrivilege.CATALOG_FULL_METADATA;
import static org.apache.polaris.core.entity.PolarisPrivilege.CATALOG_LIST;
import static org.apache.polaris.core.entity.PolarisPrivilege.CATALOG_LIST_GRANTS;
import static org.apache.polaris.core.entity.PolarisPrivilege.CATALOG_MANAGE_ACCESS;
import static org.apache.polaris.core.entity.PolarisPrivilege.CATALOG_MANAGE_CONTENT;
import static org.apache.polaris.core.entity.PolarisPrivilege.CATALOG_MANAGE_GRANTS_ON_SECURABLE;
import static org.apache.polaris.core.entity.PolarisPrivilege.CATALOG_MANAGE_METADATA;
import static org.apache.polaris.core.entity.PolarisPrivilege.CATALOG_READ_PROPERTIES;
import static org.apache.polaris.core.entity.PolarisPrivilege.CATALOG_ROLE_CREATE;
import static org.apache.polaris.core.entity.PolarisPrivilege.CATALOG_ROLE_DROP;
import static org.apache.polaris.core.entity.PolarisPrivilege.CATALOG_ROLE_FULL_METADATA;
import static org.apache.polaris.core.entity.PolarisPrivilege.CATALOG_ROLE_LIST;
import static org.apache.polaris.core.entity.PolarisPrivilege.CATALOG_ROLE_LIST_GRANTS;
import static org.apache.polaris.core.entity.PolarisPrivilege.CATALOG_ROLE_MANAGE_GRANTS_FOR_GRANTEE;
import static org.apache.polaris.core.entity.PolarisPrivilege.CATALOG_ROLE_MANAGE_GRANTS_ON_SECURABLE;
import static org.apache.polaris.core.entity.PolarisPrivilege.CATALOG_ROLE_READ_PROPERTIES;
import static org.apache.polaris.core.entity.PolarisPrivilege.CATALOG_ROLE_USAGE;
import static org.apache.polaris.core.entity.PolarisPrivilege.CATALOG_ROLE_WRITE_PROPERTIES;
import static org.apache.polaris.core.entity.PolarisPrivilege.CATALOG_WRITE_MAINTENANCE_PROPERTIES;
import static org.apache.polaris.core.entity.PolarisPrivilege.CATALOG_WRITE_PROPERTIES;
import static org.apache.polaris.core.entity.PolarisPrivilege.NAMESPACE_CREATE;
import static org.apache.polaris.core.entity.PolarisPrivilege.NAMESPACE_DROP;
import static org.apache.polaris.core.entity.PolarisPrivilege.NAMESPACE_FULL_METADATA;
import static org.apache.polaris.core.entity.PolarisPrivilege.NAMESPACE_LIST;
import static org.apache.polaris.core.entity.PolarisPrivilege.NAMESPACE_LIST_GRANTS;
import static org.apache.polaris.core.entity.PolarisPrivilege.NAMESPACE_MANAGE_GRANTS_ON_SECURABLE;
import static org.apache.polaris.core.entity.PolarisPrivilege.NAMESPACE_READ_PROPERTIES;
import static org.apache.polaris.core.entity.PolarisPrivilege.NAMESPACE_WRITE_PROPERTIES;
import static org.apache.polaris.core.entity.PolarisPrivilege.PRINCIPAL_CREATE;
import static org.apache.polaris.core.entity.PolarisPrivilege.PRINCIPAL_DROP;
import static org.apache.polaris.core.entity.PolarisPrivilege.PRINCIPAL_FULL_METADATA;
import static org.apache.polaris.core.entity.PolarisPrivilege.PRINCIPAL_LIST;
import static org.apache.polaris.core.entity.PolarisPrivilege.PRINCIPAL_LIST_GRANTS;
import static org.apache.polaris.core.entity.PolarisPrivilege.PRINCIPAL_MANAGE_GRANTS_FOR_GRANTEE;
import static org.apache.polaris.core.entity.PolarisPrivilege.PRINCIPAL_MANAGE_GRANTS_ON_SECURABLE;
import static org.apache.polaris.core.entity.PolarisPrivilege.PRINCIPAL_READ_PROPERTIES;
import static org.apache.polaris.core.entity.PolarisPrivilege.PRINCIPAL_RESET_CREDENTIALS;
import static org.apache.polaris.core.entity.PolarisPrivilege.PRINCIPAL_ROLE_CREATE;
import static org.apache.polaris.core.entity.PolarisPrivilege.PRINCIPAL_ROLE_DROP;
import static org.apache.polaris.core.entity.PolarisPrivilege.PRINCIPAL_ROLE_FULL_METADATA;
import static org.apache.polaris.core.entity.PolarisPrivilege.PRINCIPAL_ROLE_LIST;
import static org.apache.polaris.core.entity.PolarisPrivilege.PRINCIPAL_ROLE_LIST_GRANTS;
import static org.apache.polaris.core.entity.PolarisPrivilege.PRINCIPAL_ROLE_MANAGE_GRANTS_FOR_GRANTEE;
import static org.apache.polaris.core.entity.PolarisPrivilege.PRINCIPAL_ROLE_MANAGE_GRANTS_ON_SECURABLE;
import static org.apache.polaris.core.entity.PolarisPrivilege.PRINCIPAL_ROLE_READ_PROPERTIES;
import static org.apache.polaris.core.entity.PolarisPrivilege.PRINCIPAL_ROLE_USAGE;
import static org.apache.polaris.core.entity.PolarisPrivilege.PRINCIPAL_ROLE_WRITE_PROPERTIES;
import static org.apache.polaris.core.entity.PolarisPrivilege.PRINCIPAL_ROTATE_CREDENTIALS;
import static org.apache.polaris.core.entity.PolarisPrivilege.PRINCIPAL_WRITE_PROPERTIES;
import static org.apache.polaris.core.entity.PolarisPrivilege.SERVICE_MANAGE_ACCESS;
import static org.apache.polaris.core.entity.PolarisPrivilege.TABLE_CREATE;
import static org.apache.polaris.core.entity.PolarisPrivilege.TABLE_DROP;
import static org.apache.polaris.core.entity.PolarisPrivilege.TABLE_FULL_METADATA;
import static org.apache.polaris.core.entity.PolarisPrivilege.TABLE_LIST;
import static org.apache.polaris.core.entity.PolarisPrivilege.TABLE_LIST_GRANTS;
import static org.apache.polaris.core.entity.PolarisPrivilege.TABLE_MANAGE_GRANTS_ON_SECURABLE;
import static org.apache.polaris.core.entity.PolarisPrivilege.TABLE_READ_DATA;
import static org.apache.polaris.core.entity.PolarisPrivilege.TABLE_READ_PROPERTIES;
import static org.apache.polaris.core.entity.PolarisPrivilege.TABLE_WRITE_DATA;
import static org.apache.polaris.core.entity.PolarisPrivilege.TABLE_WRITE_PROPERTIES;
import static org.apache.polaris.core.entity.PolarisPrivilege.VIEW_CREATE;
import static org.apache.polaris.core.entity.PolarisPrivilege.VIEW_DROP;
import static org.apache.polaris.core.entity.PolarisPrivilege.VIEW_FULL_METADATA;
import static org.apache.polaris.core.entity.PolarisPrivilege.VIEW_LIST;
import static org.apache.polaris.core.entity.PolarisPrivilege.VIEW_LIST_GRANTS;
import static org.apache.polaris.core.entity.PolarisPrivilege.VIEW_MANAGE_GRANTS_ON_SECURABLE;
import static org.apache.polaris.core.entity.PolarisPrivilege.VIEW_READ_PROPERTIES;
import static org.apache.polaris.core.entity.PolarisPrivilege.VIEW_WRITE_PROPERTIES;

import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.polaris.core.PolarisConfiguration;
import org.apache.polaris.core.PolarisConfigurationStore;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntityConstants;
import org.apache.polaris.core.entity.PolarisEntityCore;
import org.apache.polaris.core.entity.PolarisGrantRecord;
import org.apache.polaris.core.entity.PolarisPrivilege;
import org.apache.polaris.core.persistence.PolarisResolvedPathWrapper;
import org.apache.polaris.core.persistence.ResolvedPolarisEntity;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Performs hierarchical resolution logic by matching the transively expanded set of grants to a
 * calling principal against the cascading permissions over the parent hierarchy of a target
 * Securable.
 *
 * <p>Additionally, encompasses "specialty" permission resolution logic, such as checking whether
 * the expanded roles of the calling Principal hold SERVICE_MANAGE_ACCESS on the "root" catalog,
 * which translates into a cross-catalog permission.
 */
public class PolarisAuthorizerImpl implements PolarisAuthorizer {
  private static final Logger LOGGER = LoggerFactory.getLogger(PolarisAuthorizerImpl.class);

  private static final SetMultimap<PolarisPrivilege, PolarisPrivilege> SUPER_PRIVILEGES =
      HashMultimap.create();

  static {
    SUPER_PRIVILEGES.putAll(SERVICE_MANAGE_ACCESS, List.of(SERVICE_MANAGE_ACCESS));
    SUPER_PRIVILEGES.putAll(CATALOG_MANAGE_ACCESS, List.of(CATALOG_MANAGE_ACCESS));
    SUPER_PRIVILEGES.putAll(CATALOG_ROLE_USAGE, List.of(CATALOG_ROLE_USAGE));
    SUPER_PRIVILEGES.putAll(PRINCIPAL_ROLE_USAGE, List.of(PRINCIPAL_ROLE_USAGE));

    // Namespace, Table, View privileges
    SUPER_PRIVILEGES.putAll(
        NAMESPACE_CREATE,
        List.of(
            CATALOG_MANAGE_CONTENT,
            CATALOG_MANAGE_METADATA,
            NAMESPACE_CREATE,
            NAMESPACE_FULL_METADATA));
    SUPER_PRIVILEGES.putAll(
        TABLE_CREATE,
        List.of(
            CATALOG_MANAGE_CONTENT, CATALOG_MANAGE_METADATA, TABLE_CREATE, TABLE_FULL_METADATA));
    SUPER_PRIVILEGES.putAll(
        VIEW_CREATE,
        List.of(CATALOG_MANAGE_CONTENT, CATALOG_MANAGE_METADATA, VIEW_CREATE, VIEW_FULL_METADATA));
    SUPER_PRIVILEGES.putAll(
        NAMESPACE_DROP,
        List.of(
            CATALOG_MANAGE_CONTENT,
            CATALOG_MANAGE_METADATA,
            NAMESPACE_DROP,
            NAMESPACE_FULL_METADATA));
    SUPER_PRIVILEGES.putAll(
        TABLE_DROP,
        List.of(CATALOG_MANAGE_CONTENT, CATALOG_MANAGE_METADATA, TABLE_DROP, TABLE_FULL_METADATA));
    SUPER_PRIVILEGES.putAll(
        VIEW_DROP,
        List.of(CATALOG_MANAGE_CONTENT, CATALOG_MANAGE_METADATA, VIEW_DROP, VIEW_FULL_METADATA));
    SUPER_PRIVILEGES.putAll(
        NAMESPACE_LIST,
        List.of(
            CATALOG_MANAGE_CONTENT,
            CATALOG_MANAGE_METADATA,
            NAMESPACE_CREATE,
            NAMESPACE_FULL_METADATA,
            NAMESPACE_LIST,
            NAMESPACE_READ_PROPERTIES,
            NAMESPACE_WRITE_PROPERTIES));
    SUPER_PRIVILEGES.putAll(
        TABLE_LIST,
        List.of(
            CATALOG_MANAGE_CONTENT,
            CATALOG_MANAGE_METADATA,
            TABLE_CREATE,
            TABLE_FULL_METADATA,
            TABLE_LIST,
            TABLE_READ_DATA,
            TABLE_READ_PROPERTIES,
            TABLE_WRITE_DATA,
            TABLE_WRITE_PROPERTIES));
    SUPER_PRIVILEGES.putAll(
        VIEW_LIST,
        List.of(
            CATALOG_MANAGE_CONTENT,
            CATALOG_MANAGE_METADATA,
            VIEW_CREATE,
            VIEW_FULL_METADATA,
            VIEW_LIST,
            VIEW_READ_PROPERTIES,
            VIEW_WRITE_PROPERTIES));
    SUPER_PRIVILEGES.putAll(
        NAMESPACE_READ_PROPERTIES,
        List.of(
            CATALOG_MANAGE_CONTENT,
            CATALOG_MANAGE_METADATA,
            NAMESPACE_FULL_METADATA,
            NAMESPACE_READ_PROPERTIES,
            NAMESPACE_WRITE_PROPERTIES));
    SUPER_PRIVILEGES.putAll(
        TABLE_READ_PROPERTIES,
        List.of(
            CATALOG_MANAGE_CONTENT,
            CATALOG_MANAGE_METADATA,
            TABLE_FULL_METADATA,
            TABLE_READ_DATA,
            TABLE_READ_PROPERTIES,
            TABLE_WRITE_DATA,
            TABLE_WRITE_PROPERTIES));
    SUPER_PRIVILEGES.putAll(
        VIEW_READ_PROPERTIES,
        List.of(
            CATALOG_MANAGE_CONTENT,
            CATALOG_MANAGE_METADATA,
            VIEW_FULL_METADATA,
            VIEW_READ_PROPERTIES,
            VIEW_WRITE_PROPERTIES));
    SUPER_PRIVILEGES.putAll(
        NAMESPACE_WRITE_PROPERTIES,
        List.of(
            CATALOG_MANAGE_CONTENT,
            CATALOG_MANAGE_METADATA,
            NAMESPACE_FULL_METADATA,
            NAMESPACE_WRITE_PROPERTIES));
    SUPER_PRIVILEGES.putAll(
        TABLE_WRITE_PROPERTIES,
        List.of(
            CATALOG_MANAGE_CONTENT,
            CATALOG_MANAGE_METADATA,
            TABLE_FULL_METADATA,
            TABLE_WRITE_DATA,
            TABLE_WRITE_PROPERTIES));
    SUPER_PRIVILEGES.putAll(
        VIEW_WRITE_PROPERTIES,
        List.of(
            CATALOG_MANAGE_CONTENT,
            CATALOG_MANAGE_METADATA,
            VIEW_FULL_METADATA,
            VIEW_WRITE_PROPERTIES));
    SUPER_PRIVILEGES.putAll(
        TABLE_READ_DATA, List.of(CATALOG_MANAGE_CONTENT, TABLE_READ_DATA, TABLE_WRITE_DATA));
    SUPER_PRIVILEGES.putAll(TABLE_WRITE_DATA, List.of(CATALOG_MANAGE_CONTENT, TABLE_WRITE_DATA));
    SUPER_PRIVILEGES.putAll(
        NAMESPACE_FULL_METADATA,
        List.of(CATALOG_MANAGE_CONTENT, CATALOG_MANAGE_METADATA, NAMESPACE_FULL_METADATA));
    SUPER_PRIVILEGES.putAll(
        TABLE_FULL_METADATA,
        List.of(CATALOG_MANAGE_CONTENT, CATALOG_MANAGE_METADATA, TABLE_FULL_METADATA));
    SUPER_PRIVILEGES.putAll(
        VIEW_FULL_METADATA,
        List.of(CATALOG_MANAGE_CONTENT, CATALOG_MANAGE_METADATA, VIEW_FULL_METADATA));

    // Catalog privileges
    SUPER_PRIVILEGES.putAll(
        CATALOG_MANAGE_METADATA, List.of(CATALOG_MANAGE_METADATA, CATALOG_MANAGE_CONTENT));
    SUPER_PRIVILEGES.putAll(CATALOG_MANAGE_CONTENT, List.of(CATALOG_MANAGE_CONTENT));
    SUPER_PRIVILEGES.putAll(
        CATALOG_CREATE, List.of(CATALOG_CREATE, CATALOG_FULL_METADATA, SERVICE_MANAGE_ACCESS));
    SUPER_PRIVILEGES.putAll(
        CATALOG_DROP, List.of(CATALOG_DROP, CATALOG_FULL_METADATA, SERVICE_MANAGE_ACCESS));
    SUPER_PRIVILEGES.putAll(
        CATALOG_LIST,
        List.of(
            CATALOG_CREATE,
            CATALOG_FULL_METADATA,
            CATALOG_LIST,
            CATALOG_MANAGE_CONTENT,
            CATALOG_MANAGE_METADATA,
            CATALOG_READ_PROPERTIES,
            CATALOG_WRITE_PROPERTIES,
            CATALOG_WRITE_MAINTENANCE_PROPERTIES,
            SERVICE_MANAGE_ACCESS));
    SUPER_PRIVILEGES.putAll(
        CATALOG_READ_PROPERTIES,
        List.of(
            CATALOG_FULL_METADATA,
            CATALOG_MANAGE_CONTENT,
            CATALOG_MANAGE_METADATA,
            CATALOG_READ_PROPERTIES,
            CATALOG_WRITE_PROPERTIES,
            CATALOG_WRITE_MAINTENANCE_PROPERTIES,
            SERVICE_MANAGE_ACCESS));

    // CATALOG_WRITE_MAINTENANCE_PROPERTIES is better than CATALOG_WRITE_PROPERTIES
    SUPER_PRIVILEGES.putAll(
        CATALOG_WRITE_PROPERTIES,
        List.of(
            CATALOG_FULL_METADATA,
            CATALOG_MANAGE_CONTENT,
            CATALOG_MANAGE_METADATA,
            CATALOG_WRITE_PROPERTIES,
            CATALOG_WRITE_MAINTENANCE_PROPERTIES,
            SERVICE_MANAGE_ACCESS));

    SUPER_PRIVILEGES.putAll(
        CATALOG_WRITE_MAINTENANCE_PROPERTIES,
        List.of(
            CATALOG_FULL_METADATA,
            CATALOG_MANAGE_CONTENT,
            CATALOG_MANAGE_METADATA,
            CATALOG_WRITE_MAINTENANCE_PROPERTIES,
            SERVICE_MANAGE_ACCESS));

    SUPER_PRIVILEGES.putAll(
        CATALOG_FULL_METADATA, List.of(CATALOG_FULL_METADATA, SERVICE_MANAGE_ACCESS));

    // _LIST_GRANTS
    SUPER_PRIVILEGES.putAll(
        PRINCIPAL_LIST_GRANTS,
        List.of(
            PRINCIPAL_LIST_GRANTS,
            PRINCIPAL_MANAGE_GRANTS_ON_SECURABLE,
            PRINCIPAL_MANAGE_GRANTS_FOR_GRANTEE,
            SERVICE_MANAGE_ACCESS));
    SUPER_PRIVILEGES.putAll(
        PRINCIPAL_ROLE_LIST_GRANTS,
        List.of(
            PRINCIPAL_ROLE_LIST_GRANTS,
            PRINCIPAL_ROLE_MANAGE_GRANTS_ON_SECURABLE,
            PRINCIPAL_ROLE_MANAGE_GRANTS_FOR_GRANTEE,
            SERVICE_MANAGE_ACCESS));
    SUPER_PRIVILEGES.putAll(
        CATALOG_ROLE_LIST_GRANTS,
        List.of(
            CATALOG_ROLE_LIST_GRANTS,
            CATALOG_ROLE_MANAGE_GRANTS_ON_SECURABLE,
            CATALOG_ROLE_MANAGE_GRANTS_FOR_GRANTEE,
            CATALOG_MANAGE_ACCESS));
    SUPER_PRIVILEGES.putAll(
        CATALOG_LIST_GRANTS,
        List.of(CATALOG_LIST_GRANTS, CATALOG_MANAGE_GRANTS_ON_SECURABLE, CATALOG_MANAGE_ACCESS));
    SUPER_PRIVILEGES.putAll(
        NAMESPACE_LIST_GRANTS,
        List.of(
            NAMESPACE_LIST_GRANTS, NAMESPACE_MANAGE_GRANTS_ON_SECURABLE, CATALOG_MANAGE_ACCESS));
    SUPER_PRIVILEGES.putAll(
        TABLE_LIST_GRANTS,
        List.of(TABLE_LIST_GRANTS, TABLE_MANAGE_GRANTS_ON_SECURABLE, CATALOG_MANAGE_ACCESS));
    SUPER_PRIVILEGES.putAll(
        VIEW_LIST_GRANTS,
        List.of(VIEW_LIST_GRANTS, VIEW_MANAGE_GRANTS_ON_SECURABLE, CATALOG_MANAGE_ACCESS));

    // _MANAGE_GRANTS_ON_SECURABLE for CATALOG, NAMESPACE, TABLE, VIEW
    SUPER_PRIVILEGES.putAll(
        CATALOG_MANAGE_GRANTS_ON_SECURABLE,
        List.of(CATALOG_MANAGE_GRANTS_ON_SECURABLE, CATALOG_MANAGE_ACCESS));
    SUPER_PRIVILEGES.putAll(
        NAMESPACE_MANAGE_GRANTS_ON_SECURABLE,
        List.of(NAMESPACE_MANAGE_GRANTS_ON_SECURABLE, CATALOG_MANAGE_ACCESS));
    SUPER_PRIVILEGES.putAll(
        TABLE_MANAGE_GRANTS_ON_SECURABLE,
        List.of(TABLE_MANAGE_GRANTS_ON_SECURABLE, CATALOG_MANAGE_ACCESS));
    SUPER_PRIVILEGES.putAll(
        VIEW_MANAGE_GRANTS_ON_SECURABLE,
        List.of(VIEW_MANAGE_GRANTS_ON_SECURABLE, CATALOG_MANAGE_ACCESS));

    // PRINCIPAL CRUDL
    SUPER_PRIVILEGES.putAll(
        PRINCIPAL_CREATE,
        List.of(PRINCIPAL_CREATE, PRINCIPAL_FULL_METADATA, SERVICE_MANAGE_ACCESS));
    SUPER_PRIVILEGES.putAll(
        PRINCIPAL_DROP, List.of(PRINCIPAL_DROP, PRINCIPAL_FULL_METADATA, SERVICE_MANAGE_ACCESS));
    SUPER_PRIVILEGES.putAll(
        PRINCIPAL_LIST,
        List.of(
            PRINCIPAL_LIST,
            PRINCIPAL_CREATE,
            PRINCIPAL_READ_PROPERTIES,
            PRINCIPAL_WRITE_PROPERTIES,
            PRINCIPAL_FULL_METADATA,
            SERVICE_MANAGE_ACCESS));
    SUPER_PRIVILEGES.putAll(
        PRINCIPAL_READ_PROPERTIES,
        List.of(
            PRINCIPAL_READ_PROPERTIES,
            PRINCIPAL_WRITE_PROPERTIES,
            PRINCIPAL_FULL_METADATA,
            SERVICE_MANAGE_ACCESS));
    SUPER_PRIVILEGES.putAll(
        PRINCIPAL_WRITE_PROPERTIES,
        List.of(PRINCIPAL_WRITE_PROPERTIES, PRINCIPAL_FULL_METADATA, SERVICE_MANAGE_ACCESS));
    SUPER_PRIVILEGES.putAll(
        PRINCIPAL_FULL_METADATA, List.of(PRINCIPAL_FULL_METADATA, SERVICE_MANAGE_ACCESS));

    // PRINCIPAL MANAGE_GRANTS
    SUPER_PRIVILEGES.putAll(
        PRINCIPAL_MANAGE_GRANTS_ON_SECURABLE,
        List.of(PRINCIPAL_MANAGE_GRANTS_ON_SECURABLE, SERVICE_MANAGE_ACCESS));
    SUPER_PRIVILEGES.putAll(
        PRINCIPAL_MANAGE_GRANTS_FOR_GRANTEE,
        List.of(PRINCIPAL_MANAGE_GRANTS_FOR_GRANTEE, SERVICE_MANAGE_ACCESS));

    // PRINCIPAL special privileges
    SUPER_PRIVILEGES.putAll(PRINCIPAL_ROTATE_CREDENTIALS, List.of(PRINCIPAL_ROTATE_CREDENTIALS));
    SUPER_PRIVILEGES.putAll(
        PRINCIPAL_RESET_CREDENTIALS, List.of(PRINCIPAL_RESET_CREDENTIALS, SERVICE_MANAGE_ACCESS));

    // PRINCIPAL_ROLE CRUDL
    SUPER_PRIVILEGES.putAll(
        PRINCIPAL_ROLE_CREATE,
        List.of(PRINCIPAL_ROLE_CREATE, PRINCIPAL_ROLE_FULL_METADATA, SERVICE_MANAGE_ACCESS));
    SUPER_PRIVILEGES.putAll(
        PRINCIPAL_ROLE_DROP,
        List.of(PRINCIPAL_ROLE_DROP, PRINCIPAL_ROLE_FULL_METADATA, SERVICE_MANAGE_ACCESS));
    SUPER_PRIVILEGES.putAll(
        PRINCIPAL_ROLE_LIST,
        List.of(
            PRINCIPAL_ROLE_LIST,
            PRINCIPAL_ROLE_CREATE,
            PRINCIPAL_ROLE_READ_PROPERTIES,
            PRINCIPAL_ROLE_WRITE_PROPERTIES,
            PRINCIPAL_ROLE_FULL_METADATA,
            SERVICE_MANAGE_ACCESS));
    SUPER_PRIVILEGES.putAll(
        PRINCIPAL_ROLE_READ_PROPERTIES,
        List.of(
            PRINCIPAL_ROLE_READ_PROPERTIES,
            PRINCIPAL_ROLE_WRITE_PROPERTIES,
            PRINCIPAL_ROLE_FULL_METADATA,
            SERVICE_MANAGE_ACCESS));
    SUPER_PRIVILEGES.putAll(
        PRINCIPAL_ROLE_WRITE_PROPERTIES,
        List.of(
            PRINCIPAL_ROLE_WRITE_PROPERTIES, PRINCIPAL_ROLE_FULL_METADATA, SERVICE_MANAGE_ACCESS));
    SUPER_PRIVILEGES.putAll(
        PRINCIPAL_ROLE_FULL_METADATA, List.of(PRINCIPAL_ROLE_FULL_METADATA, SERVICE_MANAGE_ACCESS));

    // PRINCIPAL_ROLE_ROLE MANAGE_GRANTS
    SUPER_PRIVILEGES.putAll(
        PRINCIPAL_ROLE_MANAGE_GRANTS_ON_SECURABLE,
        List.of(PRINCIPAL_ROLE_MANAGE_GRANTS_ON_SECURABLE, SERVICE_MANAGE_ACCESS));
    SUPER_PRIVILEGES.putAll(
        PRINCIPAL_ROLE_MANAGE_GRANTS_FOR_GRANTEE,
        List.of(PRINCIPAL_ROLE_MANAGE_GRANTS_FOR_GRANTEE, SERVICE_MANAGE_ACCESS));

    // CATALOG_ROLE CRUDL
    SUPER_PRIVILEGES.putAll(
        CATALOG_ROLE_CREATE,
        List.of(CATALOG_ROLE_CREATE, CATALOG_ROLE_FULL_METADATA, CATALOG_MANAGE_ACCESS));
    SUPER_PRIVILEGES.putAll(
        CATALOG_ROLE_DROP,
        List.of(CATALOG_ROLE_DROP, CATALOG_ROLE_FULL_METADATA, CATALOG_MANAGE_ACCESS));
    SUPER_PRIVILEGES.putAll(
        CATALOG_ROLE_LIST,
        List.of(
            CATALOG_ROLE_LIST,
            CATALOG_ROLE_CREATE,
            CATALOG_ROLE_READ_PROPERTIES,
            CATALOG_ROLE_WRITE_PROPERTIES,
            CATALOG_ROLE_FULL_METADATA,
            CATALOG_MANAGE_ACCESS));
    SUPER_PRIVILEGES.putAll(
        CATALOG_ROLE_READ_PROPERTIES,
        List.of(
            CATALOG_ROLE_READ_PROPERTIES,
            CATALOG_ROLE_WRITE_PROPERTIES,
            CATALOG_ROLE_FULL_METADATA,
            CATALOG_MANAGE_ACCESS));
    SUPER_PRIVILEGES.putAll(
        CATALOG_ROLE_WRITE_PROPERTIES,
        List.of(CATALOG_ROLE_WRITE_PROPERTIES, CATALOG_ROLE_FULL_METADATA, CATALOG_MANAGE_ACCESS));
    SUPER_PRIVILEGES.putAll(
        CATALOG_ROLE_FULL_METADATA, List.of(CATALOG_ROLE_FULL_METADATA, CATALOG_MANAGE_ACCESS));

    // CATALOG_ROLE_ROLE MANAGE_GRANTS
    SUPER_PRIVILEGES.putAll(
        CATALOG_ROLE_MANAGE_GRANTS_ON_SECURABLE,
        List.of(CATALOG_ROLE_MANAGE_GRANTS_ON_SECURABLE, CATALOG_MANAGE_ACCESS));
    SUPER_PRIVILEGES.putAll(
        CATALOG_ROLE_MANAGE_GRANTS_FOR_GRANTEE,
        List.of(CATALOG_ROLE_MANAGE_GRANTS_FOR_GRANTEE, CATALOG_MANAGE_ACCESS));
  }

  private final PolarisConfigurationStore featureConfig;

  public PolarisAuthorizerImpl(PolarisConfigurationStore featureConfig) {
    this.featureConfig = featureConfig;
  }

  /**
   * Checks whether the {@code grantedPrivilege} is sufficient to confer {@code desiredPrivilege},
   * assuming the privileges are referring to the same securable object. In other words, whether the
   * grantedPrivilege is "better than or equal to" the desiredPrivilege.
   */
  public boolean matchesOrIsSubsumedBy(
      PolarisPrivilege desiredPrivilege, PolarisPrivilege grantedPrivilege) {
    if (grantedPrivilege == desiredPrivilege) {
      return true;
    }

    if (SUPER_PRIVILEGES.containsKey(desiredPrivilege)
        && SUPER_PRIVILEGES.get(desiredPrivilege).contains(grantedPrivilege)) {
      return true;
    }
    // TODO: Fill out the map, maybe in the PolarisPrivilege enum definition itself.
    return false;
  }

  @Override
  public void authorizeOrThrow(
      @NotNull AuthenticatedPolarisPrincipal authenticatedPrincipal,
      @NotNull Set<PolarisBaseEntity> activatedEntities,
      @NotNull PolarisAuthorizableOperation authzOp,
      @Nullable PolarisResolvedPathWrapper target,
      @Nullable PolarisResolvedPathWrapper secondary) {
    authorizeOrThrow(
        authenticatedPrincipal,
        activatedEntities,
        authzOp,
        target == null ? null : List.of(target),
        secondary == null ? null : List.of(secondary));
  }

  @Override
  public void authorizeOrThrow(
      @NotNull AuthenticatedPolarisPrincipal authenticatedPrincipal,
      @NotNull Set<PolarisBaseEntity> activatedEntities,
      @NotNull PolarisAuthorizableOperation authzOp,
      @Nullable List<PolarisResolvedPathWrapper> targets,
      @Nullable List<PolarisResolvedPathWrapper> secondaries) {
    boolean enforceCredentialRotationRequiredState =
        featureConfig.getConfiguration(
            CallContext.getCurrentContext().getPolarisCallContext(),
            PolarisConfiguration.ENFORCE_PRINCIPAL_CREDENTIAL_ROTATION_REQUIRED_CHECKING);
    if (enforceCredentialRotationRequiredState
        && authenticatedPrincipal
            .getPrincipalEntity()
            .getInternalPropertiesAsMap()
            .containsKey(PolarisEntityConstants.PRINCIPAL_CREDENTIAL_ROTATION_REQUIRED_STATE)
        && authzOp != PolarisAuthorizableOperation.ROTATE_CREDENTIALS) {
      throw new ForbiddenException(
          "Principal '%s' is not authorized for op %s due to PRINCIPAL_CREDENTIAL_ROTATION_REQUIRED_STATE",
          authenticatedPrincipal.getName(), authzOp);
    } else if (!isAuthorized(
        authenticatedPrincipal, activatedEntities, authzOp, targets, secondaries)) {
      throw new ForbiddenException(
          "Principal '%s' with activated PrincipalRoles '%s' and activated grants via '%s' is not authorized for op %s",
          authenticatedPrincipal.getName(),
          authenticatedPrincipal.getActivatedPrincipalRoleNames(),
          activatedEntities.stream().map(PolarisEntityCore::getName).collect(Collectors.toSet()),
          authzOp);
    }
  }

  /**
   * Based on the required target/targetParent/secondary/secondaryParent privileges mapped from
   * {@code authzOp}, determines whether the caller's set of activatedGranteeIds is authorized for
   * the operation.
   */
  public boolean isAuthorized(
      @NotNull AuthenticatedPolarisPrincipal authenticatedPolarisPrincipal,
      @NotNull Set<PolarisBaseEntity> activatedEntities,
      @NotNull PolarisAuthorizableOperation authzOp,
      @Nullable PolarisResolvedPathWrapper target,
      @Nullable PolarisResolvedPathWrapper secondary) {
    return isAuthorized(
        authenticatedPolarisPrincipal,
        activatedEntities,
        authzOp,
        target == null ? null : List.of(target),
        secondary == null ? null : List.of(secondary));
  }

  public boolean isAuthorized(
      @NotNull AuthenticatedPolarisPrincipal authenticatedPolarisPrincipal,
      @NotNull Set<PolarisBaseEntity> activatedEntities,
      @NotNull PolarisAuthorizableOperation authzOp,
      @Nullable List<PolarisResolvedPathWrapper> targets,
      @Nullable List<PolarisResolvedPathWrapper> secondaries) {
    Set<Long> entityIdSet =
        activatedEntities.stream().map(PolarisEntityCore::getId).collect(Collectors.toSet());
    for (PolarisPrivilege privilegeOnTarget : authzOp.getPrivilegesOnTarget()) {
      // If any privileges are required on target, the target must be non-null.
      Preconditions.checkState(
          targets != null,
          "Got null target when authorizing authzOp %s for privilege %s",
          authzOp,
          privilegeOnTarget);
      for (PolarisResolvedPathWrapper target : targets) {
        if (!hasTransitivePrivilege(
            authenticatedPolarisPrincipal, entityIdSet, privilegeOnTarget, target)) {
          // TODO: Collect missing privileges to report all at the end and/or return to code
          // that throws NotAuthorizedException for more useful messages.
          return false;
        }
      }
    }
    for (PolarisPrivilege privilegeOnSecondary : authzOp.getPrivilegesOnSecondary()) {
      Preconditions.checkState(
          secondaries != null,
          "Got null secondary when authorizing authzOp %s for privilege %s",
          authzOp,
          privilegeOnSecondary);
      for (PolarisResolvedPathWrapper secondary : secondaries) {
        if (!hasTransitivePrivilege(
            authenticatedPolarisPrincipal, entityIdSet, privilegeOnSecondary, secondary)) {
          return false;
        }
      }
    }
    return true;
  }

  /**
   * Checks whether the resolvedPrincipal in the {@code resolved} resolvedPath has role-expanded
   * permissions matching {@code privilege} on any entity in the resolvedPath of the resolvedPath.
   *
   * <p>The caller is responsible for translating these checks into either behavioral actions (e.g.
   * returning 404 instead of 403, checking other root privileges that supercede the checked
   * privilege, choosing whether to vend credentials) or throwing relevant Unauthorized
   * errors/exceptions.
   */
  public boolean hasTransitivePrivilege(
      @NotNull AuthenticatedPolarisPrincipal authenticatedPolarisPrincipal,
      Set<Long> activatedGranteeIds,
      PolarisPrivilege desiredPrivilege,
      PolarisResolvedPathWrapper resolvedPath) {

    // Iterate starting at the parent, since the most common case should be to manage grants as
    // high up in the resource hierarchy as possible, so we expect earlier termination.
    for (ResolvedPolarisEntity resolvedSecurableEntity : resolvedPath.getResolvedFullPath()) {
      Preconditions.checkState(
          resolvedSecurableEntity.getGrantRecordsAsSecurable() != null,
          "Got null grantRecordsAsSecurable for resolvedSecurableEntity %s",
          resolvedSecurableEntity);
      for (PolarisGrantRecord grantRecord : resolvedSecurableEntity.getGrantRecordsAsSecurable()) {
        if (matchesOrIsSubsumedBy(
            desiredPrivilege, PolarisPrivilege.fromCode(grantRecord.getPrivilegeCode()))) {
          // Found a potential candidate for satisfying our authz goal.
          if (activatedGranteeIds.contains(grantRecord.getGranteeId())) {
            LOGGER.debug(
                "Satisfied privilege {} with grantRecord {} from securable {} for "
                    + "principalName {} and activatedIds {}",
                desiredPrivilege,
                grantRecord,
                resolvedSecurableEntity,
                authenticatedPolarisPrincipal.getName(),
                activatedGranteeIds);
            return true;
          }
        }
      }
    }

    LOGGER.debug(
        "Failed to satisfy privilege {} for principalName {} on resolvedPath {}",
        desiredPrivilege,
        authenticatedPolarisPrincipal.getName(),
        resolvedPath);
    return false;
  }
}
