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
package org.apache.polaris.core.persistence.transactional;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.time.Clock;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.core.entity.AsyncTaskType;
import org.apache.polaris.core.entity.CatalogEntity;
import org.apache.polaris.core.entity.EntityNameLookupRecord;
import org.apache.polaris.core.entity.LocationBasedEntity;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisChangeTrackingVersions;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PolarisEntityConstants;
import org.apache.polaris.core.entity.PolarisEntityCore;
import org.apache.polaris.core.entity.PolarisEntityId;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.PolarisGrantRecord;
import org.apache.polaris.core.entity.PolarisPrincipalSecrets;
import org.apache.polaris.core.entity.PolarisPrivilege;
import org.apache.polaris.core.entity.PolarisTaskConstants;
import org.apache.polaris.core.entity.PrincipalEntity;
import org.apache.polaris.core.entity.PrincipalRoleEntity;
import org.apache.polaris.core.persistence.BaseMetaStoreManager;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.core.persistence.PolarisObjectMapperUtil;
import org.apache.polaris.core.persistence.PolicyMappingAlreadyExistsException;
import org.apache.polaris.core.persistence.RetryOnConcurrencyException;
import org.apache.polaris.core.persistence.dao.entity.BaseResult;
import org.apache.polaris.core.persistence.dao.entity.ChangeTrackingResult;
import org.apache.polaris.core.persistence.dao.entity.CreateCatalogResult;
import org.apache.polaris.core.persistence.dao.entity.CreatePrincipalResult;
import org.apache.polaris.core.persistence.dao.entity.DropEntityResult;
import org.apache.polaris.core.persistence.dao.entity.EntitiesResult;
import org.apache.polaris.core.persistence.dao.entity.EntityResult;
import org.apache.polaris.core.persistence.dao.entity.EntityWithPath;
import org.apache.polaris.core.persistence.dao.entity.ListEntitiesResult;
import org.apache.polaris.core.persistence.dao.entity.LoadGrantsResult;
import org.apache.polaris.core.persistence.dao.entity.LoadPolicyMappingsResult;
import org.apache.polaris.core.persistence.dao.entity.PolicyAttachmentResult;
import org.apache.polaris.core.persistence.dao.entity.PrincipalSecretsResult;
import org.apache.polaris.core.persistence.dao.entity.PrivilegeResult;
import org.apache.polaris.core.persistence.dao.entity.ResolvedEntityResult;
import org.apache.polaris.core.persistence.dao.entity.ScopedCredentialsResult;
import org.apache.polaris.core.persistence.pagination.Page;
import org.apache.polaris.core.persistence.pagination.PageToken;
import org.apache.polaris.core.policy.PolarisPolicyMappingRecord;
import org.apache.polaris.core.policy.PolicyEntity;
import org.apache.polaris.core.policy.PolicyMappingUtil;
import org.apache.polaris.core.policy.PolicyType;
import org.apache.polaris.core.storage.PolarisStorageConfigurationInfo;
import org.apache.polaris.core.storage.PolarisStorageIntegration;
import org.apache.polaris.core.storage.StorageAccessConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default implementation of the Polaris Meta Store Manager. Uses the underlying meta store to store
 * and retrieve all Polaris metadata
 */
public class TransactionalMetaStoreManagerImpl extends BaseMetaStoreManager {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(TransactionalMetaStoreManagerImpl.class);

  private final Clock clock;

  public TransactionalMetaStoreManagerImpl(Clock clock, PolarisDiagnostics diagnostics) {
    super(diagnostics);
    this.clock = clock;
  }

  /**
   * A version of BaseMetaStoreManager::persistNewEntity but instead of calling the one-shot
   * immediate-peristence APIs of BasePersistence, expects to be run under an outer
   * runInTransaction, and calls through to analogous versions of * methods of
   * TransactionalPersistence.
   *
   * @param callCtx call context
   * @param ms meta store in read/write mode
   * @param entity entity we need a new persisted record for
   */
  protected void persistNewEntity(
      @Nonnull PolarisCallContext callCtx,
      @Nonnull TransactionalPersistence ms,
      @Nonnull PolarisBaseEntity entity) {
    // Invoke shared logic for validation and filling out remaining fields.
    entity = prepareToPersistNewEntity(callCtx, ms, entity);

    // write it
    ms.writeEntityInCurrentTxn(callCtx, entity, true, null);
  }

  /**
   * A version of BaseMetaStoreManager::persistEntityAfterChange but instead of calling the one-shot
   * immediate-peristence APIs of BasePersistence, expects to be run under an outer
   * runInTransaction, and calls through to analogous versions of * methods of
   * TransactionalPersistence.
   *
   * @param callCtx call context
   * @param ms meta store
   * @param entity the entity which has been changed
   * @param nameOrParentChanged indicates if parent or name changed
   * @param originalEntity the original state of the entity before changes
   * @return the entity with its version and lastUpdateTimestamp updated
   */
  private @Nonnull PolarisBaseEntity persistEntityAfterChange(
      @Nonnull PolarisCallContext callCtx,
      @Nonnull TransactionalPersistence ms,
      @Nonnull PolarisBaseEntity entity,
      boolean nameOrParentChanged,
      @Nonnull PolarisBaseEntity originalEntity) {
    // Invoke shared logic for validation and updating expected fields.
    entity =
        prepareToPersistEntityAfterChange(callCtx, ms, entity, nameOrParentChanged, originalEntity);

    // Use the write method defined in TransactionalPersistence which expects an
    // existing runInTransaction to already be in-place.
    ms.writeEntityInCurrentTxn(callCtx, entity, nameOrParentChanged, originalEntity);

    // return it
    return entity;
  }

  /**
   * Drop this entity. This will:
   *
   * <pre>
   *   - validate that the entity has not yet been dropped
   *   - error out if this entity is undroppable
   *   - if this is a catalog or a namespace, error out if the entity still has children
   *   - we will fully delete the entity from persistence store
   * </pre>
   *
   * @param callCtx call context
   * @param ms meta store
   * @param entity the entity being dropped
   */
  private void dropEntity(
      @Nonnull PolarisCallContext callCtx,
      @Nonnull TransactionalPersistence ms,
      @Nonnull PolarisBaseEntity entity) {

    // validate the entity type and subtype
    getDiagnostics().checkNotNull(entity, "unexpected_null_dpo");
    getDiagnostics().checkNotNull(entity.getName(), "unexpected_null_name");

    // creation timestamp must be filled
    getDiagnostics().check(entity.getDropTimestamp() == 0, "already_dropped");

    // for now drop all associated grants, etc. synchronously
    // delete ALL grant records to (if the entity is a grantee) and from that entity
    final List<PolarisGrantRecord> grantsOnGrantee =
        (entity.getType().isGrantee())
            ? ms.loadAllGrantRecordsOnGranteeInCurrentTxn(
                callCtx, entity.getCatalogId(), entity.getId())
            : List.of();
    final List<PolarisGrantRecord> grantsOnSecurable =
        ms.loadAllGrantRecordsOnSecurableInCurrentTxn(
            callCtx, entity.getCatalogId(), entity.getId());
    ms.deleteAllEntityGrantRecordsInCurrentTxn(callCtx, entity, grantsOnGrantee, grantsOnSecurable);

    // Now determine the set of entities on the other side of the grants we just removed. Grants
    // from/to these entities has been removed, hence we need to update the grant version of
    // each entity. Collect the id of each.
    Set<PolarisEntityId> entityIdsGrantChanged = new HashSet<>();
    grantsOnGrantee.forEach(
        gr ->
            entityIdsGrantChanged.add(
                new PolarisEntityId(gr.getSecurableCatalogId(), gr.getSecurableId())));
    grantsOnSecurable.forEach(
        gr ->
            entityIdsGrantChanged.add(
                new PolarisEntityId(gr.getGranteeCatalogId(), gr.getGranteeId())));

    // Bump up the grant version of these entities
    List<PolarisBaseEntity> entities =
        ms.lookupEntitiesInCurrentTxn(callCtx, new ArrayList<>(entityIdsGrantChanged));
    for (PolarisBaseEntity originalEntity : entities) {
      PolarisBaseEntity entityGrantChanged =
          originalEntity.withGrantRecordsVersion(originalEntity.getGrantRecordsVersion() + 1);
      ms.writeEntityInCurrentTxn(callCtx, entityGrantChanged, false, originalEntity);
    }

    if (entity.getType() == PolarisEntityType.POLICY
        || PolicyMappingUtil.isValidTargetEntityType(entity.getType(), entity.getSubType())) {
      // Best-effort cleanup - for policy and potential target entities, drop all policy mapping
      // records related
      try {
        final List<PolarisPolicyMappingRecord> mappingOnPolicy =
            (entity.getType() == PolarisEntityType.POLICY)
                ? ms.loadAllTargetsOnPolicyInCurrentTxn(
                    callCtx,
                    entity.getCatalogId(),
                    entity.getId(),
                    PolicyEntity.of(entity).getPolicyTypeCode())
                : List.of();
        final List<PolarisPolicyMappingRecord> mappingOnTarget =
            (entity.getType() == PolarisEntityType.POLICY)
                ? List.of()
                : ms.loadAllPoliciesOnTargetInCurrentTxn(
                    callCtx, entity.getCatalogId(), entity.getId());
        ms.deleteAllEntityPolicyMappingRecordsInCurrentTxn(
            callCtx, entity, mappingOnTarget, mappingOnPolicy);
      } catch (UnsupportedOperationException e) {
        // Policy mapping persistence not implemented, but we should not block dropping entities
      }
    }

    // remove the entity being dropped now
    ms.deleteEntityInCurrentTxn(callCtx, entity);

    // if it is a principal, we also need to drop the secrets
    if (entity.getType() == PolarisEntityType.PRINCIPAL) {
      PrincipalEntity principalEntity = PrincipalEntity.of(entity);
      String clientId = principalEntity.getClientId();
      // delete it from the secret slice
      ms.deletePrincipalSecretsInCurrentTxn(callCtx, clientId, entity.getId());
    }
    // TODO: Also, if an entity contains a storage integration, delete the storage integration
    // and other things of that nature.
  }

  /**
   * Create and persist a new grant record. This will at the same time invalidate the grant records
   * of the grantee and the securable if the grantee is a catalog role
   *
   * @param callCtx call context
   * @param ms meta store in read/write mode
   * @param securable securable
   * @param grantee grantee, either a catalog role, a principal role or a principal
   * @param priv privilege
   * @return new grant record which was created and persisted
   */
  private @Nonnull PolarisGrantRecord persistNewGrantRecord(
      @Nonnull PolarisCallContext callCtx,
      @Nonnull TransactionalPersistence ms,
      @Nonnull PolarisEntityCore securable,
      @Nonnull PolarisEntityCore grantee,
      @Nonnull PolarisPrivilege priv) {

    // validate non null arguments
    getDiagnostics().checkNotNull(securable, "unexpected_null_securable");
    getDiagnostics().checkNotNull(grantee, "unexpected_null_grantee");
    getDiagnostics().checkNotNull(priv, "unexpected_null_priv");

    // ensure that this entity is indeed a grantee like entity
    getDiagnostics()
        .check(grantee.getType().isGrantee(), "entity_must_be_grantee", "entity={}", grantee);

    // create new grant record
    PolarisGrantRecord grantRecord =
        new PolarisGrantRecord(
            securable.getCatalogId(),
            securable.getId(),
            grantee.getCatalogId(),
            grantee.getId(),
            priv.getCode());

    // persist the new grant
    ms.writeToGrantRecordsInCurrentTxn(callCtx, grantRecord);

    // load the grantee (either a catalog/principal role or a principal) and increment its grants
    // version
    PolarisBaseEntity granteeEntity =
        ms.lookupEntityInCurrentTxn(
            callCtx, grantee.getCatalogId(), grantee.getId(), grantee.getTypeCode());
    getDiagnostics().checkNotNull(granteeEntity, "grantee_not_found", "grantee={}", grantee);
    // grants have changed, we need to bump-up the grants version
    PolarisBaseEntity updatedGranteeEntity =
        granteeEntity.withGrantRecordsVersion(granteeEntity.getGrantRecordsVersion() + 1);
    ms.writeEntityInCurrentTxn(callCtx, updatedGranteeEntity, false, granteeEntity);

    // we also need to invalidate the grants on that securable so that we can reload them.
    // load the securable and increment its grants version
    PolarisBaseEntity securableEntity =
        ms.lookupEntityInCurrentTxn(
            callCtx, securable.getCatalogId(), securable.getId(), securable.getTypeCode());
    getDiagnostics()
        .checkNotNull(securableEntity, "securable_not_found", "securable={}", securable);
    // grants have changed, we need to bump-up the grants version
    PolarisBaseEntity updatedSecurableEntity =
        new PolarisBaseEntity.Builder(securableEntity)
            .grantRecordsVersion(securableEntity.getGrantRecordsVersion() + 1)
            .build();
    ms.writeEntityInCurrentTxn(callCtx, updatedSecurableEntity, false, securableEntity);

    // TODO: Update this to be an atomic bulk-update of the grantee/securable, ideally along
    // with adding the grant record in the same bulk-update.

    // done, return the new grant record
    return grantRecord;
  }

  /**
   * Delete the specified grant record from the GRANT_RECORDS table. This will at the same time
   * invalidate the grant records of the grantee and the securable if the grantee is a role
   *
   * @param callCtx call context
   * @param ms meta store
   * @param securable the securable entity
   * @param grantee the grantee entity
   * @param grantRecord the grant record to remove, which was read in the same transaction
   */
  private void revokeGrantRecord(
      @Nonnull PolarisCallContext callCtx,
      @Nonnull TransactionalPersistence ms,
      @Nonnull PolarisEntityCore securable,
      @Nonnull PolarisEntityCore grantee,
      @Nonnull PolarisGrantRecord grantRecord) {

    // validate securable
    getDiagnostics()
        .check(
            securable.getCatalogId() == grantRecord.getSecurableCatalogId()
                && securable.getId() == grantRecord.getSecurableId(),
            "securable_mismatch",
            "securable={} grantRec={}",
            securable,
            grantRecord);

    // validate grantee
    getDiagnostics()
        .check(
            grantee.getCatalogId() == grantRecord.getGranteeCatalogId()
                && grantee.getId() == grantRecord.getGranteeId(),
            "grantee_mismatch",
            "grantee={} grantRec={}",
            grantee,
            grantRecord);

    // ensure the grantee is really a grantee
    getDiagnostics().check(grantee.getType().isGrantee(), "not_a_grantee", "grantee={}", grantee);

    // remove that grant
    ms.deleteFromGrantRecordsInCurrentTxn(callCtx, grantRecord);

    // load the grantee and increment its grants version
    PolarisBaseEntity refreshGrantee =
        ms.lookupEntityInCurrentTxn(
            callCtx, grantee.getCatalogId(), grantee.getId(), grantee.getTypeCode());
    getDiagnostics()
        .checkNotNull(
            refreshGrantee, "missing_grantee", "grantRecord={} grantee={}", grantRecord, grantee);
    // grants have changed, we need to bump-up the grants version
    PolarisBaseEntity updatedRefreshGrantee =
        refreshGrantee.withGrantRecordsVersion(refreshGrantee.getGrantRecordsVersion() + 1);
    ms.writeEntityInCurrentTxn(callCtx, updatedRefreshGrantee, false, refreshGrantee);

    // we also need to invalidate the grants on that securable so that we can reload them.
    // load the securable and increment its grants version
    PolarisBaseEntity refreshSecurable =
        ms.lookupEntityInCurrentTxn(
            callCtx, securable.getCatalogId(), securable.getId(), securable.getTypeCode());
    getDiagnostics()
        .checkNotNull(
            refreshSecurable,
            "missing_securable",
            "grantRecord={} securable={}",
            grantRecord,
            securable);
    // grants have changed, we need to bump-up the grants version
    PolarisBaseEntity updatedRefreshSecurable =
        refreshSecurable.withGrantRecordsVersion(refreshSecurable.getGrantRecordsVersion() + 1);
    ms.writeEntityInCurrentTxn(callCtx, updatedRefreshSecurable, false, refreshSecurable);

    // TODO: Update this to be an atomic bulk-update of the grantee/securable, ideally along
    // with removing the grant record in the same bulk-update.
  }

  /**
   * Create a new catalog. This not only creates the new catalog entity but also the initial admin
   * role required to admin this catalog.
   *
   * @param callCtx call context
   * @param ms meta store in read/write mode
   * @param catalog the catalog entity to create
   * @param integration the storage integration that should be attached to the catalog. If null, do
   *     nothing, otherwise persist the integration.
   * @param principalRoles once the catalog has been created, list of principal roles to grant its
   *     catalog_admin role to. If no principal role is specified, we will grant the catalog_admin
   *     role of the newly created catalog to the service admin role.
   * @return the catalog we just created and its associated admin catalog role or error if we failed
   *     to
   */
  private @Nonnull CreateCatalogResult createCatalog(
      @Nonnull PolarisCallContext callCtx,
      @Nonnull TransactionalPersistence ms,
      @Nonnull PolarisBaseEntity catalog,
      @Nullable PolarisStorageIntegration<?> integration,
      @Nonnull List<PolarisEntityCore> principalRoles) {
    // validate input
    getDiagnostics().checkNotNull(catalog, "unexpected_null_catalog");

    // check if that catalog has already been created
    PolarisBaseEntity refreshCatalog =
        ms.lookupEntityInCurrentTxn(
            callCtx, catalog.getCatalogId(), catalog.getId(), catalog.getTypeCode());

    // if found, probably a retry, simply return the previously created catalog
    if (refreshCatalog != null) {
      // if found, ensure it is indeed a catalog
      getDiagnostics()
          .check(
              refreshCatalog.getTypeCode() == PolarisEntityType.CATALOG.getCode(),
              "not_a_catalog",
              "catalog={}",
              catalog);

      // lookup catalog admin role, should exist
      PolarisBaseEntity catalogAdminRole =
          ms.lookupEntityByNameInCurrentTxn(
              callCtx,
              refreshCatalog.getId(),
              refreshCatalog.getId(),
              PolarisEntityType.CATALOG_ROLE.getCode(),
              PolarisEntityConstants.getNameOfCatalogAdminRole());

      // if found, ensure not null
      getDiagnostics()
          .checkNotNull(
              catalogAdminRole, "catalog_admin_role_not_found", "catalog={}", refreshCatalog);

      // done, return the existing catalog
      return new CreateCatalogResult(refreshCatalog, catalogAdminRole);
    }

    // check that a catalog with the same name does not exist already
    // if it exists, this is an error, the client should retry
    if (ms.lookupEntityIdAndSubTypeByNameInCurrentTxn(
            callCtx,
            PolarisEntityConstants.getNullId(),
            PolarisEntityConstants.getRootEntityId(),
            PolarisEntityType.CATALOG.getCode(),
            catalog.getName())
        != null) {
      return new CreateCatalogResult(BaseResult.ReturnStatus.ENTITY_ALREADY_EXISTS, null);
    }

    ms.persistStorageIntegrationIfNeededInCurrentTxn(callCtx, catalog, integration);

    // now create and persist new catalog entity
    this.persistNewEntity(callCtx, ms, catalog);

    // create the catalog admin role for this new catalog
    long adminRoleId = ms.generateNewIdInCurrentTxn(callCtx);
    PolarisBaseEntity adminRole =
        new PolarisBaseEntity(
            catalog.getId(),
            adminRoleId,
            PolarisEntityType.CATALOG_ROLE,
            PolarisEntitySubType.NULL_SUBTYPE,
            catalog.getId(),
            PolarisEntityConstants.getNameOfCatalogAdminRole());
    this.persistNewEntity(callCtx, ms, adminRole);

    // grant the catalog admin role access-management on the catalog
    this.persistNewGrantRecord(
        callCtx, ms, catalog, adminRole, PolarisPrivilege.CATALOG_MANAGE_ACCESS);

    // grant the catalog admin role metadata-management on the catalog; this one
    // is revocable
    this.persistNewGrantRecord(
        callCtx, ms, catalog, adminRole, PolarisPrivilege.CATALOG_MANAGE_METADATA);

    // immediately assign its catalog_admin role
    if (principalRoles.isEmpty()) {
      // lookup service admin role, should exist
      PolarisBaseEntity serviceAdminRole =
          ms.lookupEntityByNameInCurrentTxn(
              callCtx,
              PolarisEntityConstants.getNullId(),
              PolarisEntityConstants.getRootEntityId(),
              PolarisEntityType.PRINCIPAL_ROLE.getCode(),
              PolarisEntityConstants.getNameOfPrincipalServiceAdminRole());
      getDiagnostics().checkNotNull(serviceAdminRole, "missing_service_admin_role");
      this.persistNewGrantRecord(
          callCtx, ms, adminRole, serviceAdminRole, PolarisPrivilege.CATALOG_ROLE_USAGE);
    } else {
      // grant to each principal role usage on its catalog_admin role
      for (PolarisEntityCore principalRole : principalRoles) {
        // validate not null and really a principal role
        getDiagnostics().checkNotNull(principalRole, "null principal role");
        getDiagnostics()
            .check(
                principalRole.getTypeCode() == PolarisEntityType.PRINCIPAL_ROLE.getCode(),
                "not_principal_role",
                "type={}",
                principalRole.getType());

        // grant usage on that catalog admin role to this principal
        this.persistNewGrantRecord(
            callCtx, ms, adminRole, principalRole, PolarisPrivilege.CATALOG_ROLE_USAGE);
      }
    }

    // success, return the two entities
    return new CreateCatalogResult(catalog, adminRole);
  }

  /**
   * Bootstrap Polaris catalog service
   *
   * @param callCtx call context
   * @param ms meta store in read/write mode
   */
  private void bootstrapPolarisService(
      @Nonnull PolarisCallContext callCtx, @Nonnull TransactionalPersistence ms) {

    // Create a root container entity that can represent the securable for any top-level grants.
    PolarisBaseEntity rootContainer =
        new PolarisBaseEntity(
            PolarisEntityConstants.getNullId(),
            PolarisEntityConstants.getRootEntityId(),
            PolarisEntityType.ROOT,
            PolarisEntitySubType.NULL_SUBTYPE,
            PolarisEntityConstants.getRootEntityId(),
            PolarisEntityConstants.getRootContainerName());
    this.persistNewEntity(callCtx, ms, rootContainer);

    // Now bootstrap the service by creating the root principal and the service_admin principal
    // role. The principal role will be granted to that root principal and the root catalog admin
    // of the root catalog will be granted to that principal role.
    long rootPrincipalId = ms.generateNewIdInCurrentTxn(callCtx);
    PrincipalEntity rootPrincipal =
        new PrincipalEntity.Builder()
            .setId(rootPrincipalId)
            .setName(PolarisEntityConstants.getRootPrincipalName())
            .setCreateTimestamp(System.currentTimeMillis())
            .build();
    this.createPrincipal(callCtx, ms, rootPrincipal);

    // now create the account admin principal role
    long serviceAdminPrincipalRoleId = ms.generateNewIdInCurrentTxn(callCtx);
    PrincipalRoleEntity serviceAdminPrincipalRole =
        new PrincipalRoleEntity.Builder()
            .setId(serviceAdminPrincipalRoleId)
            .setName(PolarisEntityConstants.getNameOfPrincipalServiceAdminRole())
            .setCreateTimestamp(System.currentTimeMillis())
            .build();
    this.persistNewEntity(callCtx, ms, serviceAdminPrincipalRole);

    // we also need to grant usage on the account-admin principal to the principal
    this.persistNewGrantRecord(
        callCtx,
        ms,
        serviceAdminPrincipalRole,
        rootPrincipal,
        PolarisPrivilege.PRINCIPAL_ROLE_USAGE);

    // grant SERVICE_MANAGE_ACCESS on the rootContainer to the serviceAdminPrincipalRole
    this.persistNewGrantRecord(
        callCtx,
        ms,
        rootContainer,
        serviceAdminPrincipalRole,
        PolarisPrivilege.SERVICE_MANAGE_ACCESS);
  }

  /** {@inheritDoc} */
  @Override
  public @Nonnull BaseResult bootstrapPolarisService(@Nonnull PolarisCallContext callCtx) {
    // get meta store we should be using
    TransactionalPersistence ms = ((TransactionalPersistence) callCtx.getMetaStore());

    // run operation in a read/write transaction
    ms.runActionInTransaction(callCtx, () -> this.bootstrapPolarisService(callCtx, ms));

    // all good
    return new BaseResult(BaseResult.ReturnStatus.SUCCESS);
  }

  @Override
  public @Nonnull BaseResult purge(@Nonnull PolarisCallContext callCtx) {
    // get meta store we should be using
    TransactionalPersistence ms = ((TransactionalPersistence) callCtx.getMetaStore());

    // run operation in a read/write transaction
    LOGGER.warn("Deleting all metadata in the metastore...");
    ms.runActionInTransaction(callCtx, () -> ms.deleteAllInCurrentTxn(callCtx));
    LOGGER.warn("Finished deleting all metadata in the metastore");

    // all good
    return new BaseResult(BaseResult.ReturnStatus.SUCCESS);
  }

  /**
   * See {@link #readEntityByName(PolarisCallContext, List, PolarisEntityType, PolarisEntitySubType,
   * String)}
   */
  private @Nonnull EntityResult readEntityByName(
      @Nonnull PolarisCallContext callCtx,
      @Nonnull TransactionalPersistence ms,
      @Nullable List<PolarisEntityCore> catalogPath,
      @Nonnull PolarisEntityType entityType,
      @Nonnull PolarisEntitySubType entitySubType,
      @Nonnull String name) {
    // first resolve again the catalogPath to that entity
    PolarisEntityResolver resolver =
        new PolarisEntityResolver(getDiagnostics(), callCtx, ms, catalogPath);

    // return if we failed to resolve
    if (resolver.isFailure()) {
      return new EntityResult(BaseResult.ReturnStatus.CATALOG_PATH_CANNOT_BE_RESOLVED, null);
    }

    // now looking the entity by name
    PolarisBaseEntity entity =
        ms.lookupEntityByNameInCurrentTxn(
            callCtx,
            resolver.getCatalogIdOrNull(),
            resolver.getParentId(),
            entityType.getCode(),
            name);

    // if found, check if subType really matches
    if (entity != null
        && entitySubType != PolarisEntitySubType.ANY_SUBTYPE
        && entity.getSubTypeCode() != entitySubType.getCode()) {
      entity = null;
    }

    // success, return what we found
    return (entity == null)
        ? new EntityResult(BaseResult.ReturnStatus.ENTITY_NOT_FOUND, null)
        : new EntityResult(entity);
  }

  /** {@inheritDoc} */
  @Override
  public @Nonnull EntityResult readEntityByName(
      @Nonnull PolarisCallContext callCtx,
      @Nullable List<PolarisEntityCore> catalogPath,
      @Nonnull PolarisEntityType entityType,
      @Nonnull PolarisEntitySubType entitySubType,
      @Nonnull String name) {
    // get meta store we should be using
    TransactionalPersistence ms = ((TransactionalPersistence) callCtx.getMetaStore());

    // run operation in a read/write transaction
    return ms.runInReadTransaction(
        callCtx, () -> readEntityByName(callCtx, ms, catalogPath, entityType, entitySubType, name));
  }

  /**
   * See {@link PolarisMetaStoreManager#listEntities(PolarisCallContext, List, PolarisEntityType,
   * PolarisEntitySubType, PageToken)}
   */
  private @Nonnull ListEntitiesResult listEntities(
      @Nonnull PolarisCallContext callCtx,
      @Nonnull TransactionalPersistence ms,
      @Nullable List<PolarisEntityCore> catalogPath,
      @Nonnull PolarisEntityType entityType,
      @Nonnull PolarisEntitySubType entitySubType,
      @Nonnull PageToken pageToken) {
    // first resolve again the catalogPath to that entity
    PolarisEntityResolver resolver =
        new PolarisEntityResolver(getDiagnostics(), callCtx, ms, catalogPath);

    // return if we failed to resolve
    if (resolver.isFailure()) {
      return new ListEntitiesResult(BaseResult.ReturnStatus.CATALOG_PATH_CANNOT_BE_RESOLVED, null);
    }

    // return list of active entities
    Page<EntityNameLookupRecord> resultPage =
        ms.listEntitiesInCurrentTxn(
            callCtx,
            resolver.getCatalogIdOrNull(),
            resolver.getParentId(),
            entityType,
            entitySubType,
            pageToken);

    return ListEntitiesResult.fromPage(resultPage);
  }

  /** {@inheritDoc} */
  @Override
  public @Nonnull ListEntitiesResult listEntities(
      @Nonnull PolarisCallContext callCtx,
      @Nullable List<PolarisEntityCore> catalogPath,
      @Nonnull PolarisEntityType entityType,
      @Nonnull PolarisEntitySubType entitySubType,
      @Nonnull PageToken pageToken) {
    // get meta store we should be using
    TransactionalPersistence ms = ((TransactionalPersistence) callCtx.getMetaStore());

    // run operation in a read transaction
    return ms.runInReadTransaction(
        callCtx,
        () -> listEntities(callCtx, ms, catalogPath, entityType, entitySubType, pageToken));
  }

  /**
   * See {@link PolarisMetaStoreManager#loadEntities(PolarisCallContext, List, PolarisEntityType,
   * PolarisEntitySubType, PageToken)}
   */
  private @Nonnull Page<PolarisBaseEntity> loadEntities(
      @Nonnull PolarisCallContext callCtx,
      @Nonnull TransactionalPersistence ms,
      @Nullable List<PolarisEntityCore> catalogPath,
      @Nonnull PolarisEntityType entityType,
      @Nonnull PolarisEntitySubType entitySubType,
      @Nonnull PageToken pageToken) {
    // first resolve again the catalogPath to that entity
    PolarisEntityResolver resolver =
        new PolarisEntityResolver(getDiagnostics(), callCtx, ms, catalogPath);

    // throw if we failed to resolve
    if (resolver.isFailure()) {
      throw new IllegalArgumentException("Failed to resolve catalogPath: " + catalogPath);
    }

    // return list of active entities
    return ms.loadEntitiesInCurrentTxn(
        callCtx,
        resolver.getCatalogIdOrNull(),
        resolver.getParentId(),
        entityType,
        entitySubType,
        entity -> true,
        Function.identity(),
        pageToken);
  }

  /** {@inheritDoc} */
  @Override
  public @Nonnull Page<PolarisBaseEntity> loadEntities(
      @Nonnull PolarisCallContext callCtx,
      @Nullable List<PolarisEntityCore> catalogPath,
      @Nonnull PolarisEntityType entityType,
      @Nonnull PolarisEntitySubType entitySubType,
      @Nonnull PageToken pageToken) {
    // get meta store we should be using
    TransactionalPersistence ms = ((TransactionalPersistence) callCtx.getMetaStore());

    // run operation in a read transaction
    return ms.runInReadTransaction(
        callCtx,
        () -> loadEntities(callCtx, ms, catalogPath, entityType, entitySubType, pageToken));
  }

  /** {@link #createPrincipal(PolarisCallContext, PrincipalEntity)} */
  private @Nonnull CreatePrincipalResult createPrincipal(
      @Nonnull PolarisCallContext callCtx,
      @Nonnull TransactionalPersistence ms,
      @Nonnull PrincipalEntity principal) {
    // validate input
    getDiagnostics().checkNotNull(principal, "unexpected_null_principal");

    // check if that catalog has already been created
    PrincipalEntity refreshPrincipal =
        PrincipalEntity.of(
            ms.lookupEntityInCurrentTxn(
                callCtx, principal.getCatalogId(), principal.getId(), principal.getTypeCode()));

    // if found, probably a retry, simply return the previously created principal
    if (refreshPrincipal != null) {
      String clientId = refreshPrincipal.getClientId();
      getDiagnostics().checkNotNull(clientId, "null_client_id", "principal={}", refreshPrincipal);
      getDiagnostics()
          .check(!clientId.isEmpty(), "empty_client_id", "principal={}", refreshPrincipal);

      // get the main and secondary secrets for that client
      PolarisPrincipalSecrets principalSecrets =
          ms.loadPrincipalSecretsInCurrentTxn(callCtx, clientId);

      // should not be null
      getDiagnostics()
          .checkNotNull(
              principalSecrets,
              "missing_principal_secrets",
              "clientId={} principal={}",
              clientId,
              refreshPrincipal);

      // done, return the newly created principal
      return new CreatePrincipalResult(refreshPrincipal, principalSecrets);
    }

    // check that a principal with the same name does not exist already
    // if it exists, this is an error, the client should retry
    if (ms.lookupEntityIdAndSubTypeByNameInCurrentTxn(
            callCtx,
            PolarisEntityConstants.getNullId(),
            PolarisEntityConstants.getRootEntityId(),
            PolarisEntityType.PRINCIPAL.getCode(),
            principal.getName())
        != null) {
      return new CreatePrincipalResult(BaseResult.ReturnStatus.ENTITY_ALREADY_EXISTS, null);
    }

    // generate new secrets for this principal
    PolarisPrincipalSecrets principalSecrets =
        ms.generateNewPrincipalSecretsInCurrentTxn(callCtx, principal.getName(), principal.getId());

    // remember client id
    PrincipalEntity updatedPrincipal =
        new PrincipalEntity.Builder(principal)
            .setClientId(principalSecrets.getPrincipalClientId())
            .build();

    // now create and persist new catalog entity
    this.persistNewEntity(callCtx, ms, updatedPrincipal);

    // success, return the two entities
    return new CreatePrincipalResult(updatedPrincipal, principalSecrets);
  }

  /** {@inheritDoc} */
  @Override
  public @Nonnull CreatePrincipalResult createPrincipal(
      @Nonnull PolarisCallContext callCtx, @Nonnull PrincipalEntity principal) {
    // get metastore we should be using
    TransactionalPersistence ms = ((TransactionalPersistence) callCtx.getMetaStore());

    // need to run inside a read/write transaction
    return ms.runInTransaction(callCtx, () -> this.createPrincipal(callCtx, ms, principal));
  }

  /** See {@link #loadPrincipalSecrets(PolarisCallContext, String)} */
  private @Nullable PolarisPrincipalSecrets loadPrincipalSecrets(
      @Nonnull PolarisCallContext callCtx, TransactionalPersistence ms, @Nonnull String clientId) {
    return ms.loadPrincipalSecretsInCurrentTxn(callCtx, clientId);
  }

  /** {@inheritDoc} */
  @Override
  public @Nonnull PrincipalSecretsResult loadPrincipalSecrets(
      @Nonnull PolarisCallContext callCtx, @Nonnull String clientId) {
    // get metastore we should be using
    TransactionalPersistence ms = ((TransactionalPersistence) callCtx.getMetaStore());

    // need to run inside a read/write transaction
    PolarisPrincipalSecrets secrets =
        ms.runInTransaction(callCtx, () -> this.loadPrincipalSecrets(callCtx, ms, clientId));

    return (secrets == null)
        ? new PrincipalSecretsResult(BaseResult.ReturnStatus.ENTITY_NOT_FOUND, null)
        : new PrincipalSecretsResult(secrets);
  }

  /** {@inheritDoc} */
  @Override
  public @Nonnull void deletePrincipalSecrets(
      @Nonnull PolarisCallContext callCtx, @Nonnull String clientId, long principalId) {
    // get metastore we should be using
    TransactionalPersistence ms = ((TransactionalPersistence) callCtx.getMetaStore());

    // need to run inside a read/write transaction
    ms.deletePrincipalSecrets(callCtx, clientId, principalId);
  }

  /** See {@link #} */
  private @Nullable PolarisPrincipalSecrets rotatePrincipalSecrets(
      @Nonnull PolarisCallContext callCtx,
      @Nonnull TransactionalPersistence ms,
      @Nonnull String clientId,
      long principalId,
      boolean reset,
      @Nonnull String oldSecretHash) {
    // if not found, the principal must have been dropped
    EntityResult loadEntityResult =
        loadEntity(
            callCtx,
            ms,
            PolarisEntityConstants.getNullId(),
            principalId,
            PolarisEntityType.PRINCIPAL.getCode());
    if (loadEntityResult.getReturnStatus() != BaseResult.ReturnStatus.SUCCESS) {
      return null;
    }

    PolarisBaseEntity principal = loadEntityResult.getEntity();
    PolarisBaseEntity.Builder principalBuilder = new PolarisBaseEntity.Builder(principal);
    Map<String, String> internalProps = principal.getInternalPropertiesAsMap();

    boolean doReset =
        reset
            || internalProps.get(
                    PolarisEntityConstants.PRINCIPAL_CREDENTIAL_ROTATION_REQUIRED_STATE)
                != null;
    PolarisPrincipalSecrets secrets =
        ms.rotatePrincipalSecretsInCurrentTxn(
            callCtx, clientId, principalId, doReset, oldSecretHash);

    if (reset
        && !internalProps.containsKey(
            PolarisEntityConstants.PRINCIPAL_CREDENTIAL_ROTATION_REQUIRED_STATE)) {
      internalProps.put(
          PolarisEntityConstants.PRINCIPAL_CREDENTIAL_ROTATION_REQUIRED_STATE, "true");
      principalBuilder.internalPropertiesAsMap(internalProps);
      principalBuilder.entityVersion(principal.getEntityVersion() + 1);
      ms.writeEntityInCurrentTxn(callCtx, principalBuilder.build(), true, principal);
    } else if (internalProps.containsKey(
        PolarisEntityConstants.PRINCIPAL_CREDENTIAL_ROTATION_REQUIRED_STATE)) {
      internalProps.remove(PolarisEntityConstants.PRINCIPAL_CREDENTIAL_ROTATION_REQUIRED_STATE);
      principalBuilder.internalPropertiesAsMap(internalProps);
      principalBuilder.entityVersion(principal.getEntityVersion() + 1);
      ms.writeEntityInCurrentTxn(callCtx, principalBuilder.build(), true, principal);
    }
    return secrets;
  }

  /** {@inheritDoc} */
  @Override
  public @Nonnull PrincipalSecretsResult rotatePrincipalSecrets(
      @Nonnull PolarisCallContext callCtx,
      @Nonnull String clientId,
      long principalId,
      boolean reset,
      @Nonnull String oldSecretHash) {
    // get metastore we should be using
    TransactionalPersistence ms = ((TransactionalPersistence) callCtx.getMetaStore());

    // need to run inside a read/write transaction
    PolarisPrincipalSecrets secrets =
        ms.runInTransaction(
            callCtx,
            () ->
                this.rotatePrincipalSecrets(
                    callCtx, ms, clientId, principalId, reset, oldSecretHash));

    return (secrets == null)
        ? new PrincipalSecretsResult(BaseResult.ReturnStatus.ENTITY_NOT_FOUND, null)
        : new PrincipalSecretsResult(secrets);
  }

  private @Nullable PolarisPrincipalSecrets resetPrincipalSecrets(
      @Nonnull PolarisCallContext callCtx,
      @Nonnull TransactionalPersistence ms,
      @Nonnull String clientId,
      long principalId,
      String customClientId,
      String customClientSecret) {
    PolarisPrincipalSecrets secrets =
        ms.storePrincipalSecrets(callCtx, principalId, customClientId, customClientSecret);
    return secrets;
  }

  @Override
  public @Nonnull PrincipalSecretsResult resetPrincipalSecrets(
      @Nonnull PolarisCallContext callCtx,
      long principalId,
      @Nonnull String resolvedClientId,
      String customClientSecret) {
    // get metastore we should be using
    TransactionalPersistence ms = ((TransactionalPersistence) callCtx.getMetaStore());
    // if not found, the principal must have been dropped
    EntityResult loadEntityResult =
        loadEntity(
            callCtx, PolarisEntityConstants.getNullId(), principalId, PolarisEntityType.PRINCIPAL);
    if (loadEntityResult.getReturnStatus() != BaseResult.ReturnStatus.SUCCESS) {
      return new PrincipalSecretsResult(BaseResult.ReturnStatus.ENTITY_NOT_FOUND, null);
    }

    // need to run inside a read/write transaction
    PolarisPrincipalSecrets secrets =
        ms.runInTransaction(
            callCtx,
            () ->
                this.resetPrincipalSecrets(
                    callCtx,
                    ms,
                    resolvedClientId,
                    principalId,
                    resolvedClientId,
                    customClientSecret));

    return (secrets == null)
        ? new PrincipalSecretsResult(BaseResult.ReturnStatus.ENTITY_NOT_FOUND, null)
        : new PrincipalSecretsResult(secrets);
  }

  /** {@inheritDoc} */
  @Override
  public @Nonnull CreateCatalogResult createCatalog(
      @Nonnull PolarisCallContext callCtx,
      @Nonnull PolarisBaseEntity catalog,
      @Nonnull List<PolarisEntityCore> principalRoles) {
    // get metastore we should be using
    TransactionalPersistence ms = ((TransactionalPersistence) callCtx.getMetaStore());

    Map<String, String> internalProp = catalog.getInternalPropertiesAsMap();
    String integrationIdentifierOrId =
        internalProp.get(PolarisEntityConstants.getStorageIntegrationIdentifierPropertyName());
    String storageConfigInfoStr =
        internalProp.get(PolarisEntityConstants.getStorageConfigInfoPropertyName());
    PolarisStorageIntegration<?> integration;
    // storageConfigInfo's presence is needed to create a storage integration
    // and the catalog should not have an internal property of storage identifier or id yet
    if (storageConfigInfoStr != null && integrationIdentifierOrId == null) {
      integration =
          ms.createStorageIntegrationInCurrentTxn(
              callCtx,
              catalog.getCatalogId(),
              catalog.getId(),
              PolarisStorageConfigurationInfo.deserialize(storageConfigInfoStr));
    } else {
      integration = null;
    }
    // need to run inside a read/write transaction
    return ms.runInTransaction(
        callCtx, () -> this.createCatalog(callCtx, ms, catalog, integration, principalRoles));
  }

  /** {@link #createEntityIfNotExists(PolarisCallContext, List, PolarisBaseEntity)} */
  private @Nonnull EntityResult createEntityIfNotExists(
      @Nonnull PolarisCallContext callCtx,
      @Nonnull TransactionalPersistence ms,
      @Nullable List<PolarisEntityCore> catalogPath,
      @Nonnull PolarisBaseEntity entity) {

    // entity cannot be null
    getDiagnostics().checkNotNull(entity, "unexpected_null_entity");

    // entity name must be specified
    getDiagnostics().checkNotNull(entity.getName(), "unexpected_null_entity_name");

    // first, check if the entity has already been created, in which case we will simply return it
    PolarisBaseEntity entityFound =
        ms.lookupEntityInCurrentTxn(
            callCtx, entity.getCatalogId(), entity.getId(), entity.getTypeCode());
    if (entityFound != null) {
      // probably the client retried, simply return it
      // TODO: Check correctness of returning entityFound vs entity here. It may have already
      // been updated after the creation.
      return new EntityResult(entityFound);
    }

    // first resolve again the catalogPath
    PolarisEntityResolver resolver =
        new PolarisEntityResolver(getDiagnostics(), callCtx, ms, catalogPath);

    // return if we failed to resolve
    if (resolver.isFailure()) {
      return new EntityResult(BaseResult.ReturnStatus.CATALOG_PATH_CANNOT_BE_RESOLVED, null);
    }

    // check if an entity does not already exist with the same name. If true, this is an error
    EntityNameLookupRecord entityActiveRecord =
        ms.lookupEntityIdAndSubTypeByNameInCurrentTxn(
            callCtx,
            entity.getCatalogId(),
            entity.getParentId(),
            entity.getType().getCode(),
            entity.getName());
    if (entityActiveRecord != null) {
      return new EntityResult(
          BaseResult.ReturnStatus.ENTITY_ALREADY_EXISTS, entityActiveRecord.getSubTypeCode());
    }

    // persist that new entity
    this.persistNewEntity(callCtx, ms, entity);

    // done, return that newly created entity
    return new EntityResult(entity);
  }

  /** {@inheritDoc} */
  @Override
  public @Nonnull EntityResult createEntityIfNotExists(
      @Nonnull PolarisCallContext callCtx,
      @Nullable List<PolarisEntityCore> catalogPath,
      @Nonnull PolarisBaseEntity entity) {
    // get metastore we should be using
    TransactionalPersistence ms = ((TransactionalPersistence) callCtx.getMetaStore());

    // need to run inside a read/write transaction
    return ms.runInTransaction(
        callCtx, () -> this.createEntityIfNotExists(callCtx, ms, catalogPath, entity));
  }

  @Override
  public @Nonnull EntitiesResult createEntitiesIfNotExist(
      @Nonnull PolarisCallContext callCtx,
      @Nullable List<PolarisEntityCore> catalogPath,
      @Nonnull List<? extends PolarisBaseEntity> entities) {
    // get metastore we should be using
    TransactionalPersistence ms = ((TransactionalPersistence) callCtx.getMetaStore());

    // need to run inside a read/write transaction
    return ms.runInTransaction(
        callCtx,
        () -> {
          List<PolarisBaseEntity> createdEntities = new ArrayList<>(entities.size());
          for (PolarisBaseEntity entity : entities) {
            EntityResult entityCreateResult =
                createEntityIfNotExists(callCtx, ms, catalogPath, entity);
            // abort everything if error
            if (entityCreateResult.getReturnStatus() != BaseResult.ReturnStatus.SUCCESS) {
              ms.rollback();
              return new EntitiesResult(
                  entityCreateResult.getReturnStatus(), entityCreateResult.getExtraInformation());
            }
            createdEntities.add(entityCreateResult.getEntity());
          }
          return new EntitiesResult(Page.fromItems(createdEntities));
        });
  }

  /**
   * See {@link #updateEntityPropertiesIfNotChanged(PolarisCallContext, List, PolarisBaseEntity)}
   */
  private @Nonnull EntityResult updateEntityPropertiesIfNotChanged(
      @Nonnull PolarisCallContext callCtx,
      @Nonnull TransactionalPersistence ms,
      @Nullable List<PolarisEntityCore> catalogPath,
      @Nonnull PolarisBaseEntity entity) {
    // entity cannot be null
    getDiagnostics().checkNotNull(entity, "unexpected_null_entity");

    // re-resolve everything including that entity
    PolarisEntityResolver resolver =
        new PolarisEntityResolver(getDiagnostics(), callCtx, ms, catalogPath, entity);

    // if resolution failed, return false
    if (resolver.isFailure()) {
      return new EntityResult(BaseResult.ReturnStatus.CATALOG_PATH_CANNOT_BE_RESOLVED, null);
    }

    // lookup the entity, cannot be null
    PolarisBaseEntity entityRefreshed =
        ms.lookupEntityInCurrentTxn(
            callCtx, entity.getCatalogId(), entity.getId(), entity.getTypeCode());
    getDiagnostics()
        .checkNotNull(entityRefreshed, "unexpected_entity_not_found", "entity={}", entity);

    // check that the version of the entity has not changed at all to avoid concurrent updates
    if (entityRefreshed.getEntityVersion() != entity.getEntityVersion()) {
      // TODO: Check if this is a bug that might cause grantRecordVersions to go backwards. The
      // behavior for grantRecordVersions updates could be either to still allow the update
      // while taking the higher grantRecordsVersion, or to hard-fail. If we still allow the
      // update, then we're only guaranteeing that the caller was authorized for the update
      // *at some point in time* in the vicinity of an updated grant. If we don't have retries
      // that re-fetch the entity properties and try to re-apply updates server-side this is
      // probably fine, but if we do have server-side retries *after* grantRecordVersion
      // resolution, this could mean a bug where a grant is revoked before something else
      // already modifies the table in a way that is only safe with that grant revoked,
      // while the retry will allow the update to go through. If we consider grantRecordsVersion
      // a strict part of the version comparison here then we have well-ordering guarantees.
      // TODO: Give useful extraInformation
      return new EntityResult(BaseResult.ReturnStatus.TARGET_ENTITY_CONCURRENTLY_MODIFIED, null);
    }
    // update the two properties
    PolarisBaseEntity updatedEntity =
        new PolarisBaseEntity.Builder(entityRefreshed)
            .internalProperties(entity.getInternalProperties())
            .properties(entity.getProperties())
            .build();
    // persist this entity after changing it. This will update the version and update the last
    // updated time. Because the entity version is changed, we will update the change tracking table
    PolarisBaseEntity persistedEntity =
        this.persistEntityAfterChange(callCtx, ms, updatedEntity, false, entityRefreshed);
    return new EntityResult(persistedEntity);
  }

  /** {@inheritDoc} */
  @Override
  public @Nonnull EntityResult updateEntityPropertiesIfNotChanged(
      @Nonnull PolarisCallContext callCtx,
      @Nullable List<PolarisEntityCore> catalogPath,
      @Nonnull PolarisBaseEntity entity) {
    // get metastore we should be using
    TransactionalPersistence ms = ((TransactionalPersistence) callCtx.getMetaStore());

    // need to run inside a read/write transaction
    return ms.runInTransaction(
        callCtx, () -> this.updateEntityPropertiesIfNotChanged(callCtx, ms, catalogPath, entity));
  }

  /** See {@link #updateEntitiesPropertiesIfNotChanged(PolarisCallContext, List)} */
  private @Nonnull EntitiesResult updateEntitiesPropertiesIfNotChanged(
      @Nonnull PolarisCallContext callCtx,
      @Nonnull TransactionalPersistence ms,
      @Nonnull List<EntityWithPath> entities) {
    // ensure that the entities list is not null
    getDiagnostics().checkNotNull(entities, "unexpected_null_entities");

    // list of all updated entities
    List<PolarisBaseEntity> updatedEntities = new ArrayList<>(entities.size());

    // iterate over the list and update each, one at a time
    for (EntityWithPath entityWithPath : entities) {
      // update that entity, abort if it fails
      EntityResult updatedEntityResult =
          this.updateEntityPropertiesIfNotChanged(
              callCtx, ms, entityWithPath.getCatalogPath(), entityWithPath.getEntity());

      // if failed, rollback and return the last error
      if (updatedEntityResult.getReturnStatus() != BaseResult.ReturnStatus.SUCCESS) {
        ms.rollback();
        return new EntitiesResult(
            updatedEntityResult.getReturnStatus(), updatedEntityResult.getExtraInformation());
      }

      // one more was updated
      updatedEntities.add(updatedEntityResult.getEntity());
    }

    // good, all success
    return new EntitiesResult(Page.fromItems(updatedEntities));
  }

  /** {@inheritDoc} */
  @Override
  public @Nonnull EntitiesResult updateEntitiesPropertiesIfNotChanged(
      @Nonnull PolarisCallContext callCtx, @Nonnull List<EntityWithPath> entities) {
    // get metastore we should be using
    TransactionalPersistence ms = ((TransactionalPersistence) callCtx.getMetaStore());

    // need to run inside a read/write transaction
    return ms.runInTransaction(
        callCtx, () -> this.updateEntitiesPropertiesIfNotChanged(callCtx, ms, entities));
  }

  /**
   * See {@link PolarisMetaStoreManager#renameEntity(PolarisCallContext, List, PolarisBaseEntity,
   * List, PolarisEntity)}
   */
  private @Nonnull EntityResult renameEntity(
      @Nonnull PolarisCallContext callCtx,
      @Nonnull TransactionalPersistence ms,
      @Nullable List<PolarisEntityCore> catalogPath,
      @Nonnull PolarisEntityCore entityToRename,
      @Nullable List<PolarisEntityCore> newCatalogPath,
      @Nonnull PolarisBaseEntity renamedEntity) {

    // entity and new name cannot be null
    getDiagnostics().checkNotNull(entityToRename, "unexpected_null_entityToRename");
    getDiagnostics().checkNotNull(renamedEntity, "unexpected_null_renamedEntity");

    // if a new catalog path is specified (i.e. re-parent operation), a catalog path should be
    // specified too
    getDiagnostics()
        .check(
            (newCatalogPath == null) || (catalogPath != null),
            "newCatalogPath_specified_without_catalogPath");

    // null is shorthand for saying the path isn't changing
    if (newCatalogPath == null) {
      newCatalogPath = catalogPath;
    }

    // re-resolve everything including that entity
    PolarisEntityResolver resolver =
        new PolarisEntityResolver(getDiagnostics(), callCtx, ms, catalogPath, entityToRename);

    // if resolution failed, return false
    if (resolver.isFailure()) {
      return new EntityResult(BaseResult.ReturnStatus.ENTITY_CANNOT_BE_RESOLVED, null);
    }

    // find the entity to rename
    PolarisBaseEntity refreshEntityToRename =
        ms.lookupEntityInCurrentTxn(
            callCtx,
            entityToRename.getCatalogId(),
            entityToRename.getId(),
            entityToRename.getTypeCode());

    // if this entity was not found, return failure. Not expected here because it was
    // resolved successfully (see above)
    if (refreshEntityToRename == null) {
      return new EntityResult(BaseResult.ReturnStatus.ENTITY_NOT_FOUND, null);
    }

    // check that the source entity has not changed since it was updated by the caller
    if (refreshEntityToRename.getEntityVersion() != renamedEntity.getEntityVersion()) {
      return new EntityResult(BaseResult.ReturnStatus.TARGET_ENTITY_CONCURRENTLY_MODIFIED, null);
    }

    // ensure it can be renamed
    if (refreshEntityToRename.cannotBeDroppedOrRenamed()) {
      return new EntityResult(BaseResult.ReturnStatus.ENTITY_CANNOT_BE_RENAMED, null);
    }

    // re-resolve the new catalog path if this entity is going to be moved
    if (newCatalogPath != null) {
      resolver = new PolarisEntityResolver(getDiagnostics(), callCtx, ms, newCatalogPath);

      // if resolution failed, return false
      if (resolver.isFailure()) {
        return new EntityResult(BaseResult.ReturnStatus.CATALOG_PATH_CANNOT_BE_RESOLVED, null);
      }
    }

    // ensure that nothing exists where we create that entity
    // if this entity already exists, this is an error
    EntityNameLookupRecord entityActiveRecord =
        ms.lookupEntityIdAndSubTypeByNameInCurrentTxn(
            callCtx,
            resolver.getCatalogIdOrNull(),
            resolver.getParentId(),
            refreshEntityToRename.getTypeCode(),
            renamedEntity.getName());
    if (entityActiveRecord != null) {
      return new EntityResult(
          BaseResult.ReturnStatus.ENTITY_ALREADY_EXISTS, entityActiveRecord.getSubTypeCode());
    }

    // Create a copy of the original before we change its fields so that we can pass in the
    // old version for the persistence layer to work out whether to unlink previous name-lookups
    PolarisBaseEntity.Builder updatedEntityBuilder =
        new PolarisBaseEntity.Builder(refreshEntityToRename);

    // change its name now
    updatedEntityBuilder.name(renamedEntity.getName());
    updatedEntityBuilder.properties(renamedEntity.getProperties());
    updatedEntityBuilder.internalProperties(renamedEntity.getInternalProperties());

    // re-parent if a new catalog path was specified
    if (newCatalogPath != null) {
      updatedEntityBuilder.parentId(resolver.getParentId());
    }

    // persist the entity after change. This will update the lastUpdateTimestamp and bump up the
    // version. Indicate that the nameOrParent changed, so that we also update any by-name
    // lookups if applicable
    PolarisBaseEntity renamedEntityToReturn =
        this.persistEntityAfterChange(
            callCtx, ms, updatedEntityBuilder.build(), true, refreshEntityToRename);
    return new EntityResult(renamedEntityToReturn);
  }

  /** {@inheritDoc} */
  @Override
  public @Nonnull EntityResult renameEntity(
      @Nonnull PolarisCallContext callCtx,
      @Nullable List<PolarisEntityCore> catalogPath,
      @Nonnull PolarisBaseEntity entityToRename,
      @Nullable List<PolarisEntityCore> newCatalogPath,
      @Nonnull PolarisEntity renamedEntity) {
    // get metastore we should be using
    TransactionalPersistence ms = ((TransactionalPersistence) callCtx.getMetaStore());

    // need to run inside a read/write transaction
    return ms.runInTransaction(
        callCtx,
        () ->
            this.renameEntity(
                callCtx, ms, catalogPath, entityToRename, newCatalogPath, renamedEntity));
  }

  /**
   * See
   *
   * <p>{@link #dropEntityIfExists(PolarisCallContext, TransactionalPersistence, List,
   * PolarisBaseEntity, Map, boolean)}
   */
  private @Nonnull DropEntityResult dropEntityIfExists(
      @Nonnull PolarisCallContext callCtx,
      @Nonnull TransactionalPersistence ms,
      @Nullable List<PolarisEntityCore> catalogPath,
      @Nonnull PolarisBaseEntity entityToDrop,
      @Nullable Map<String, String> cleanupProperties,
      boolean cleanup) {
    // entity cannot be null
    getDiagnostics().checkNotNull(entityToDrop, "unexpected_null_entity");

    // re-resolve everything including that entity
    PolarisEntityResolver resolver =
        new PolarisEntityResolver(getDiagnostics(), callCtx, ms, catalogPath, entityToDrop);

    // if resolution failed, return false
    if (resolver.isFailure()) {
      return new DropEntityResult(BaseResult.ReturnStatus.CATALOG_PATH_CANNOT_BE_RESOLVED, null);
    }

    // first find the entity to drop
    PolarisBaseEntity refreshEntityToDrop =
        ms.lookupEntityInCurrentTxn(
            callCtx, entityToDrop.getCatalogId(), entityToDrop.getId(), entityToDrop.getTypeCode());

    // if this entity was not found, return failure
    if (refreshEntityToDrop == null) {
      return new DropEntityResult(BaseResult.ReturnStatus.ENTITY_NOT_FOUND, null);
    }

    // ensure that this entity is droppable
    if (refreshEntityToDrop.cannotBeDroppedOrRenamed()) {
      return new DropEntityResult(BaseResult.ReturnStatus.ENTITY_UNDROPPABLE, null);
    }

    // check that the entity has children, in which case it is an error. This only applies to
    // a namespaces or a catalog
    if (refreshEntityToDrop.getType() == PolarisEntityType.CATALOG) {
      // the id of the catalog
      long catalogId = refreshEntityToDrop.getId();
      CatalogEntity catalogEntityToDrop = CatalogEntity.of(refreshEntityToDrop);

      // For passthrough facade catalogs, all catalog level entities, except catalog roles, are
      // passthrough entities that are not source-of-truth
      // TODO: Temporarily allow dropping a catalog with passthrough entities remaining, in the long
      // term we need to cleanup them up to avoid accumulation in the metastore
      if (!catalogEntityToDrop.isPassthroughFacade()
          || !callCtx
              .getRealmConfig()
              .getConfig(
                  FeatureConfiguration.ALLOW_DROPPING_NON_EMPTY_PASSTHROUGH_FACADE_CATALOG,
                  catalogEntityToDrop)) {
        // if not all namespaces are dropped, we cannot drop this catalog
        if (ms.hasChildrenInCurrentTxn(
            callCtx, PolarisEntityType.NAMESPACE, catalogId, catalogId)) {
          return new DropEntityResult(
              BaseResult.ReturnStatus.NAMESPACE_NOT_EMPTY,
              catalogEntityToDrop.isPassthroughFacade()
                  ? String.format(
                      "Set %s to true to drop non-empty passthrough facade catalogs",
                      FeatureConfiguration.ALLOW_DROPPING_NON_EMPTY_PASSTHROUGH_FACADE_CATALOG
                          .key())
                  : null);
        }
      }

      // get the list of catalog roles, at most 2
      List<PolarisBaseEntity> catalogRoles =
          ms.loadEntitiesInCurrentTxn(
                  callCtx,
                  catalogId,
                  catalogId,
                  PolarisEntityType.CATALOG_ROLE,
                  PolarisEntitySubType.ANY_SUBTYPE,
                  entity -> true,
                  Function.identity(),
                  PageToken.fromLimit(2))
              .items();

      // if we have 2, we cannot drop the catalog. If only one left, better be the admin role
      if (catalogRoles.size() > 1) {
        return new DropEntityResult(BaseResult.ReturnStatus.CATALOG_NOT_EMPTY, null);
      }

      // if 1, drop the last catalog role. Should be the catalog admin role but don't validate this
      if (!catalogRoles.isEmpty()) {
        // drop the last catalog role in that catalog, should be the admin catalog role
        this.dropEntity(callCtx, ms, catalogRoles.get(0));
      }
    } else if (refreshEntityToDrop.getType() == PolarisEntityType.NAMESPACE) {
      if (ms.hasChildrenInCurrentTxn(
          callCtx, null, refreshEntityToDrop.getCatalogId(), refreshEntityToDrop.getId())) {
        return new DropEntityResult(BaseResult.ReturnStatus.NAMESPACE_NOT_EMPTY, null);
      }
    } else if (refreshEntityToDrop.getType() == PolarisEntityType.POLICY && !cleanup) {
      // need to check if the policy is attached to any entity
      try {
        List<PolarisPolicyMappingRecord> records =
            ms.loadAllTargetsOnPolicyInCurrentTxn(
                callCtx,
                refreshEntityToDrop.getCatalogId(),
                refreshEntityToDrop.getId(),
                PolicyEntity.of(refreshEntityToDrop).getPolicyTypeCode());
        if (!records.isEmpty()) {
          return new DropEntityResult(BaseResult.ReturnStatus.POLICY_HAS_MAPPINGS, null);
        }
      } catch (UnsupportedOperationException e) {
        // Policy mapping persistence not implemented, but we should not block dropping entities
      }
    }

    // simply delete that entity. Will be removed from entities_active, added to the
    // entities_dropped and its version will be changed.
    this.dropEntity(callCtx, ms, refreshEntityToDrop);

    // if cleanup, schedule a cleanup task for the entity. do this here, so that drop and scheduling
    // the cleanup task is transactional. Otherwise, we'll be unable to schedule the cleanup task
    // later
    if (cleanup && refreshEntityToDrop.getType() != PolarisEntityType.POLICY) {
      Map<String, String> properties = new HashMap<>();
      properties.put(
          PolarisTaskConstants.TASK_TYPE,
          String.valueOf(AsyncTaskType.ENTITY_CLEANUP_SCHEDULER.typeCode()));
      properties.put(
          PolarisTaskConstants.TASK_DATA, PolarisObjectMapperUtil.serialize(refreshEntityToDrop));
      PolarisBaseEntity.Builder taskEntityBuilder =
          new PolarisBaseEntity.Builder()
              .id(ms.generateNewIdInCurrentTxn(callCtx))
              .catalogId(0L)
              .name("entityCleanup_" + entityToDrop.getId())
              .typeCode(PolarisEntityType.TASK.getCode())
              .subTypeCode(PolarisEntitySubType.NULL_SUBTYPE.getCode())
              .createTimestamp(clock.millis())
              .propertiesAsMap(properties);
      if (cleanupProperties != null) {
        taskEntityBuilder.internalPropertiesAsMap(cleanupProperties);
      }
      PolarisBaseEntity taskEntity = taskEntityBuilder.build();
      createEntityIfNotExists(callCtx, ms, null, taskEntity);
      return new DropEntityResult(taskEntity.getId());
    }

    // done, return success
    return new DropEntityResult();
  }

  /** {@inheritDoc} */
  @Override
  public @Nonnull DropEntityResult dropEntityIfExists(
      @Nonnull PolarisCallContext callCtx,
      @Nullable List<PolarisEntityCore> catalogPath,
      @Nonnull PolarisBaseEntity entityToDrop,
      @Nullable Map<String, String> cleanupProperties,
      boolean cleanup) {
    // get metastore we should be using
    TransactionalPersistence ms = ((TransactionalPersistence) callCtx.getMetaStore());

    // need to run inside a read/write transaction
    return ms.runInTransaction(
        callCtx,
        () ->
            this.dropEntityIfExists(
                callCtx, ms, catalogPath, entityToDrop, cleanupProperties, cleanup));
  }

  /**
   * Resolve the arguments of granting/revoking a usage grant between a role (catalog or principal
   * role) and a grantee (either a principal role or a principal)
   *
   * @param callCtx call context
   * @param ms meta store in read/write mode
   * @param catalog if the role is a catalog role, the caller needs to pass-in the catalog entity
   *     which was used to resolve that role. Else null.
   * @param role the role, either a catalog or principal role
   * @param grantee the grantee
   * @return resolver for the specified entities
   */
  private @Nonnull PolarisEntityResolver resolveRoleToGranteeUsageGrant(
      @Nonnull PolarisCallContext callCtx,
      @Nonnull TransactionalPersistence ms,
      @Nullable PolarisEntityCore catalog,
      @Nonnull PolarisEntityCore role,
      @Nonnull PolarisEntityCore grantee) {

    // validate the grantee input
    getDiagnostics().checkNotNull(grantee, "unexpected_null_grantee");
    getDiagnostics().check(grantee.getType().isGrantee(), "not_a_grantee", "grantee={}", grantee);

    // validate role
    getDiagnostics().checkNotNull(role, "unexpected_null_role");

    // role should be a catalog or a principal role
    boolean isCatalogRole = role.getTypeCode() == PolarisEntityType.CATALOG_ROLE.getCode();
    boolean isPrincipalRole = role.getTypeCode() == PolarisEntityType.PRINCIPAL_ROLE.getCode();
    getDiagnostics().check(isCatalogRole || isPrincipalRole, "not_a_role");

    // if the role is a catalog role, ensure a catalog is specified and
    // vice-versa, catalog should be null if the role is a principal role
    getDiagnostics()
        .check(
            (catalog == null && isPrincipalRole) || (catalog != null && isCatalogRole),
            "catalog_mismatch",
            "catalog={} role={}",
            catalog,
            role);

    // re-resolve now all these entities
    List<PolarisEntityCore> otherTopLevelEntities = new ArrayList<>(2);
    otherTopLevelEntities.add(role);
    otherTopLevelEntities.add(grantee);

    // ensure these entities have not changed
    return new PolarisEntityResolver(
        getDiagnostics(),
        callCtx,
        ms,
        catalog != null ? List.of(catalog) : null,
        null,
        otherTopLevelEntities);
  }

  /**
   * Helper function to resolve the securable to role grant privilege
   *
   * @param grantee resolved grantee
   * @param catalogPath path to that entity, cannot be null or empty if securable has a catalogId
   * @param securable securable entity, must have been resolved by the client
   * @return a resolver for the role, the catalog path and the securable
   */
  private PolarisEntityResolver resolveSecurableToRoleGrant(
      @Nonnull PolarisCallContext callCtx,
      @Nonnull TransactionalPersistence ms,
      @Nonnull PolarisEntityCore grantee,
      @Nullable List<PolarisEntityCore> catalogPath,
      @Nonnull PolarisEntityCore securable) {
    // validate role input
    getDiagnostics().checkNotNull(grantee, "unexpected_null_grantee");
    getDiagnostics()
        .check(grantee.getType().isGrantee(), "not_grantee_type", "grantee={}", grantee);

    // securable must be supplied
    getDiagnostics().checkNotNull(securable, "unexpected_null_securable");
    if (securable.getCatalogId() > 0) {
      // catalogPath must be supplied if the securable has a catalogId
      getDiagnostics().checkNotNull(catalogPath, "unexpected_null_catalogPath");
    }

    // re-resolve now all these entities
    return new PolarisEntityResolver(
        getDiagnostics(), callCtx, ms, catalogPath, securable, List.of(grantee));
  }

  /**
   * See {@link #grantUsageOnRoleToGrantee(PolarisCallContext, PolarisEntityCore, PolarisEntityCore,
   * PolarisEntityCore)}
   */
  private @Nonnull PrivilegeResult grantUsageOnRoleToGrantee(
      @Nonnull PolarisCallContext callCtx,
      @Nonnull TransactionalPersistence ms,
      @Nullable PolarisEntityCore catalog,
      @Nonnull PolarisEntityCore role,
      @Nonnull PolarisEntityCore grantee) {

    // ensure these entities have not changed
    PolarisEntityResolver resolver =
        this.resolveRoleToGranteeUsageGrant(callCtx, ms, catalog, role, grantee);

    // if failure to resolve, let the caller know
    if (resolver.isFailure()) {
      return new PrivilegeResult(BaseResult.ReturnStatus.ENTITY_CANNOT_BE_RESOLVED, null);
    }

    // the usage privilege to grant
    PolarisPrivilege usagePriv =
        (grantee.getType() == PolarisEntityType.PRINCIPAL_ROLE)
            ? PolarisPrivilege.CATALOG_ROLE_USAGE
            : PolarisPrivilege.PRINCIPAL_ROLE_USAGE;

    // grant usage on this role to this principal
    getDiagnostics().check(grantee.getType().isGrantee(), "not_a_grantee", "grantee={}", grantee);
    PolarisGrantRecord grantRecord =
        this.persistNewGrantRecord(callCtx, ms, role, grantee, usagePriv);
    return new PrivilegeResult(grantRecord);
  }

  /** {@inheritDoc} */
  @Override
  public @Nonnull PrivilegeResult grantUsageOnRoleToGrantee(
      @Nonnull PolarisCallContext callCtx,
      @Nullable PolarisEntityCore catalog,
      @Nonnull PolarisEntityCore role,
      @Nonnull PolarisEntityCore grantee) {
    // get metastore we should be using
    TransactionalPersistence ms = ((TransactionalPersistence) callCtx.getMetaStore());

    // need to run inside a read/write transaction
    return ms.runInTransaction(
        callCtx, () -> this.grantUsageOnRoleToGrantee(callCtx, ms, catalog, role, grantee));
  }

  /**
   * See {@link #revokeUsageOnRoleFromGrantee(PolarisCallContext, PolarisEntityCore,
   * PolarisEntityCore, PolarisEntityCore)}
   */
  private @Nonnull PrivilegeResult revokeUsageOnRoleFromGrantee(
      @Nonnull PolarisCallContext callCtx,
      @Nonnull TransactionalPersistence ms,
      @Nullable PolarisEntityCore catalog,
      @Nonnull PolarisEntityCore role,
      @Nonnull PolarisEntityCore grantee) {

    // ensure these entities have not changed
    PolarisEntityResolver resolver =
        this.resolveRoleToGranteeUsageGrant(callCtx, ms, catalog, role, grantee);

    // if failure to resolve, let the caller know
    if (resolver.isFailure()) {
      return new PrivilegeResult(BaseResult.ReturnStatus.ENTITY_CANNOT_BE_RESOLVED, null);
    }

    // the usage privilege to revoke
    PolarisPrivilege usagePriv =
        (grantee.getType() == PolarisEntityType.PRINCIPAL_ROLE)
            ? PolarisPrivilege.CATALOG_ROLE_USAGE
            : PolarisPrivilege.PRINCIPAL_ROLE_USAGE;

    // first, ensure that this privilege has been granted
    PolarisGrantRecord grantRecord =
        ms.lookupGrantRecordInCurrentTxn(
            callCtx,
            role.getCatalogId(),
            role.getId(),
            grantee.getCatalogId(),
            grantee.getId(),
            usagePriv.getCode());

    // this is not a really bad error, no-op really
    if (grantRecord == null) {
      return new PrivilegeResult(BaseResult.ReturnStatus.GRANT_NOT_FOUND, null);
    }

    // revoke usage on the role from the grantee
    this.revokeGrantRecord(callCtx, ms, role, grantee, grantRecord);

    return new PrivilegeResult(grantRecord);
  }

  /** {@inheritDoc} */
  @Override
  public @Nonnull PrivilegeResult revokeUsageOnRoleFromGrantee(
      @Nonnull PolarisCallContext callCtx,
      @Nullable PolarisEntityCore catalog,
      @Nonnull PolarisEntityCore role,
      @Nonnull PolarisEntityCore grantee) {
    // get metastore we should be using
    TransactionalPersistence ms = ((TransactionalPersistence) callCtx.getMetaStore());

    // need to run inside a read/write transaction
    return ms.runInTransaction(
        callCtx, () -> this.revokeUsageOnRoleFromGrantee(callCtx, ms, catalog, role, grantee));
  }

  /**
   * See {@link #grantPrivilegeOnSecurableToRole(PolarisCallContext, PolarisEntityCore, List,
   * PolarisEntityCore, PolarisPrivilege)}
   */
  private @Nonnull PrivilegeResult grantPrivilegeOnSecurableToRole(
      @Nonnull PolarisCallContext callCtx,
      @Nonnull TransactionalPersistence ms,
      @Nonnull PolarisEntityCore grantee,
      @Nullable List<PolarisEntityCore> catalogPath,
      @Nonnull PolarisEntityCore securable,
      @Nonnull PolarisPrivilege priv) {

    // re-resolve now all these entities
    PolarisEntityResolver resolver =
        this.resolveSecurableToRoleGrant(callCtx, ms, grantee, catalogPath, securable);

    // if failure to resolve, let the caller know
    if (resolver.isFailure()) {
      return new PrivilegeResult(BaseResult.ReturnStatus.ENTITY_CANNOT_BE_RESOLVED, null);
    }

    // grant specified privilege on this securable to this role and return the grant
    PolarisGrantRecord grantRecord =
        this.persistNewGrantRecord(callCtx, ms, securable, grantee, priv);
    return new PrivilegeResult(grantRecord);
  }

  /** {@inheritDoc} */
  @Override
  public @Nonnull PrivilegeResult grantPrivilegeOnSecurableToRole(
      @Nonnull PolarisCallContext callCtx,
      @Nonnull PolarisEntityCore grantee,
      @Nullable List<PolarisEntityCore> catalogPath,
      @Nonnull PolarisEntityCore securable,
      @Nonnull PolarisPrivilege privilege) {
    // get metastore we should be using
    TransactionalPersistence ms = ((TransactionalPersistence) callCtx.getMetaStore());

    // need to run inside a read/write transaction
    return ms.runInTransaction(
        callCtx,
        () ->
            this.grantPrivilegeOnSecurableToRole(
                callCtx, ms, grantee, catalogPath, securable, privilege));
  }

  /**
   * See {@link #revokePrivilegeOnSecurableFromRole(PolarisCallContext, PolarisEntityCore, List,
   * PolarisEntityCore, PolarisPrivilege)}
   */
  private @Nonnull PrivilegeResult revokePrivilegeOnSecurableFromRole(
      @Nonnull PolarisCallContext callCtx,
      @Nonnull TransactionalPersistence ms,
      @Nonnull PolarisEntityCore grantee,
      @Nullable List<PolarisEntityCore> catalogPath,
      @Nonnull PolarisEntityCore securable,
      @Nonnull PolarisPrivilege priv) {

    // re-resolve now all these entities
    PolarisEntityResolver resolver =
        this.resolveSecurableToRoleGrant(callCtx, ms, grantee, catalogPath, securable);

    // if failure to resolve, let the caller know
    if (resolver.isFailure()) {
      return new PrivilegeResult(BaseResult.ReturnStatus.ENTITY_CANNOT_BE_RESOLVED, null);
    }

    // lookup the grants records to find this grant
    PolarisGrantRecord grantRecord =
        ms.lookupGrantRecordInCurrentTxn(
            callCtx,
            securable.getCatalogId(),
            securable.getId(),
            grantee.getCatalogId(),
            grantee.getId(),
            priv.getCode());

    // the grant does not exist, nothing to do really
    if (grantRecord == null) {
      return new PrivilegeResult(BaseResult.ReturnStatus.GRANT_NOT_FOUND, null);
    }

    // revoke the specified privilege on this securable from this role
    this.revokeGrantRecord(callCtx, ms, securable, grantee, grantRecord);

    // success!
    return new PrivilegeResult(grantRecord);
  }

  /** {@inheritDoc} */
  @Override
  public @Nonnull PrivilegeResult revokePrivilegeOnSecurableFromRole(
      @Nonnull PolarisCallContext callCtx,
      @Nonnull PolarisEntityCore grantee,
      @Nullable List<PolarisEntityCore> catalogPath,
      @Nonnull PolarisEntityCore securable,
      @Nonnull PolarisPrivilege privilege) {
    // get metastore we should be using
    TransactionalPersistence ms = ((TransactionalPersistence) callCtx.getMetaStore());

    // need to run inside a read/write transaction
    return ms.runInTransaction(
        callCtx,
        () ->
            this.revokePrivilegeOnSecurableFromRole(
                callCtx, ms, grantee, catalogPath, securable, privilege));
  }

  /** {@link #loadGrantsOnSecurable(PolarisCallContext, long, long)} */
  private @Nonnull LoadGrantsResult loadGrantsOnSecurable(
      @Nonnull PolarisCallContext callCtx,
      @Nonnull TransactionalPersistence ms,
      long securableCatalogId,
      long securableId) {

    // lookup grants version for this securable entity
    int grantsVersion =
        ms.lookupEntityGrantRecordsVersionInCurrentTxn(callCtx, securableCatalogId, securableId);

    // return null if securable does not exists
    if (grantsVersion == 0) {
      return new LoadGrantsResult(BaseResult.ReturnStatus.ENTITY_NOT_FOUND, null);
    }

    // now fetch all grants for this securable
    final List<PolarisGrantRecord> returnGrantRecords =
        ms.loadAllGrantRecordsOnSecurableInCurrentTxn(callCtx, securableCatalogId, securableId);

    // find all unique grantees
    List<PolarisEntityId> entityIds =
        returnGrantRecords.stream()
            .map(
                grantRecord ->
                    new PolarisEntityId(
                        grantRecord.getGranteeCatalogId(), grantRecord.getGranteeId()))
            .distinct()
            .collect(Collectors.toList());
    List<PolarisBaseEntity> entities = ms.lookupEntitiesInCurrentTxn(callCtx, entityIds);

    // done, return the list of grants and their version
    return new LoadGrantsResult(
        grantsVersion,
        returnGrantRecords,
        entities.stream().filter(Objects::nonNull).collect(Collectors.toList()));
  }

  /** {@inheritDoc} */
  @Override
  public @Nonnull LoadGrantsResult loadGrantsOnSecurable(
      @Nonnull PolarisCallContext callCtx, PolarisEntityCore securable) {
    return loadGrantsOnSecurable(callCtx, securable.getCatalogId(), securable.getId());
  }

  public @Nonnull LoadGrantsResult loadGrantsOnSecurable(
      @Nonnull PolarisCallContext callCtx, long securableCatalogId, long securableId) {
    // get metastore we should be using
    TransactionalPersistence ms = ((TransactionalPersistence) callCtx.getMetaStore());

    // need to run inside a read transaction
    return ms.runInReadTransaction(
        callCtx, () -> this.loadGrantsOnSecurable(callCtx, ms, securableCatalogId, securableId));
  }

  /** {@link #loadGrantsToGrantee(PolarisCallContext, long, long)} */
  public @Nonnull LoadGrantsResult loadGrantsToGrantee(
      @Nonnull PolarisCallContext callCtx,
      @Nonnull TransactionalPersistence ms,
      long granteeCatalogId,
      long granteeId) {

    // lookup grants version for this grantee entity
    int grantsVersion =
        ms.lookupEntityGrantRecordsVersionInCurrentTxn(callCtx, granteeCatalogId, granteeId);

    // return null if grantee does not exists
    if (grantsVersion == 0) {
      return new LoadGrantsResult(BaseResult.ReturnStatus.ENTITY_NOT_FOUND, null);
    }

    // now fetch all grants for this grantee
    final List<PolarisGrantRecord> returnGrantRecords =
        ms.loadAllGrantRecordsOnGranteeInCurrentTxn(callCtx, granteeCatalogId, granteeId);

    // find all unique securables
    List<PolarisEntityId> entityIds =
        returnGrantRecords.stream()
            .map(
                grantRecord ->
                    new PolarisEntityId(
                        grantRecord.getSecurableCatalogId(), grantRecord.getSecurableId()))
            .distinct()
            .collect(Collectors.toList());
    List<PolarisBaseEntity> entities = ms.lookupEntitiesInCurrentTxn(callCtx, entityIds);

    // done, return the list of grants and their version
    return new LoadGrantsResult(
        grantsVersion,
        returnGrantRecords,
        entities.stream().filter(Objects::nonNull).collect(Collectors.toList()));
  }

  /** {@inheritDoc} */
  @Override
  public @Nonnull LoadGrantsResult loadGrantsToGrantee(
      @Nonnull PolarisCallContext callCtx, PolarisEntityCore grantee) {
    return loadGrantsToGrantee(callCtx, grantee.getCatalogId(), grantee.getId());
  }

  public @Nonnull LoadGrantsResult loadGrantsToGrantee(
      @Nonnull PolarisCallContext callCtx, long granteeCatalogId, long granteeId) {
    // get metastore we should be using
    TransactionalPersistence ms = ((TransactionalPersistence) callCtx.getMetaStore());

    // need to run inside a read transaction
    return ms.runInReadTransaction(
        callCtx, () -> this.loadGrantsToGrantee(callCtx, ms, granteeCatalogId, granteeId));
  }

  /** {@link PolarisMetaStoreManager#loadEntitiesChangeTracking(PolarisCallContext, List)} */
  private @Nonnull ChangeTrackingResult loadEntitiesChangeTracking(
      @Nonnull PolarisCallContext callCtx,
      @Nonnull TransactionalPersistence ms,
      @Nonnull List<PolarisEntityId> entityIds) {
    List<PolarisChangeTrackingVersions> changeTracking =
        ms.lookupEntityVersionsInCurrentTxn(callCtx, entityIds);
    return new ChangeTrackingResult(changeTracking);
  }

  /** {@inheritDoc} */
  @Override
  public @Nonnull ChangeTrackingResult loadEntitiesChangeTracking(
      @Nonnull PolarisCallContext callCtx, @Nonnull List<PolarisEntityId> entityIds) {
    // get metastore we should be using
    TransactionalPersistence ms = ((TransactionalPersistence) callCtx.getMetaStore());

    // need to run inside a read transaction
    return ms.runInReadTransaction(
        callCtx, () -> this.loadEntitiesChangeTracking(callCtx, ms, entityIds));
  }

  /** Refer to {@link #loadEntity(PolarisCallContext, long, long, PolarisEntityType)} */
  private @Nonnull EntityResult loadEntity(
      @Nonnull PolarisCallContext callCtx,
      @Nonnull TransactionalPersistence ms,
      long entityCatalogId,
      long entityId,
      int entityTypeCode) {
    // this is an easy one
    PolarisBaseEntity entity =
        ms.lookupEntityInCurrentTxn(callCtx, entityCatalogId, entityId, entityTypeCode);
    return (entity != null)
        ? new EntityResult(entity)
        : new EntityResult(BaseResult.ReturnStatus.ENTITY_NOT_FOUND, null);
  }

  /** {@inheritDoc} */
  @Override
  public @Nonnull EntityResult loadEntity(
      @Nonnull PolarisCallContext callCtx,
      long entityCatalogId,
      long entityId,
      @Nonnull PolarisEntityType entityType) {
    // get metastore we should be using
    TransactionalPersistence ms = ((TransactionalPersistence) callCtx.getMetaStore());

    // need to run inside a read transaction
    return ms.runInReadTransaction(
        callCtx,
        () -> this.loadEntity(callCtx, ms, entityCatalogId, entityId, entityType.getCode()));
  }

  /** Refer to {@link #loadTasks(PolarisCallContext, String, PageToken)} */
  private @Nonnull EntitiesResult loadTasks(
      @Nonnull PolarisCallContext callCtx,
      @Nonnull TransactionalPersistence ms,
      String executorId,
      PageToken pageToken) {

    // find all available tasks
    Page<PolarisBaseEntity> availableTasks =
        ms.loadEntitiesInCurrentTxn(
            callCtx,
            PolarisEntityConstants.getRootEntityId(),
            PolarisEntityConstants.getRootEntityId(),
            PolarisEntityType.TASK,
            PolarisEntitySubType.ANY_SUBTYPE,
            entity -> {
              PolarisObjectMapperUtil.TaskExecutionState taskState =
                  PolarisObjectMapperUtil.parseTaskState(entity);
              long taskAgeTimeout =
                  callCtx
                      .getRealmConfig()
                      .getConfig(
                          PolarisTaskConstants.TASK_TIMEOUT_MILLIS_CONFIG,
                          PolarisTaskConstants.TASK_TIMEOUT_MILLIS);
              return taskState == null
                  || taskState.executor == null
                  || clock.millis() - taskState.lastAttemptStartTime > taskAgeTimeout;
            },
            Function.identity(),
            pageToken);

    List<PolarisBaseEntity> loadedTasks =
        availableTasks.items().stream()
            .map(
                task -> {
                  PolarisBaseEntity.Builder updatedTask = new PolarisBaseEntity.Builder(task);
                  Map<String, String> properties = task.getPropertiesAsMap();
                  properties.put(PolarisTaskConstants.LAST_ATTEMPT_EXECUTOR_ID, executorId);
                  properties.put(
                      PolarisTaskConstants.LAST_ATTEMPT_START_TIME, String.valueOf(clock.millis()));
                  properties.put(
                      PolarisTaskConstants.ATTEMPT_COUNT,
                      String.valueOf(
                          Integer.parseInt(
                                  properties.getOrDefault(PolarisTaskConstants.ATTEMPT_COUNT, "0"))
                              + 1));
                  updatedTask.propertiesAsMap(properties);
                  EntityResult result =
                      updateEntityPropertiesIfNotChanged(callCtx, ms, null, updatedTask.build());
                  if (result.getReturnStatus() == BaseResult.ReturnStatus.SUCCESS) {
                    return result.getEntity();
                  } else {
                    // TODO: Consider performing incremental leasing of individual tasks one at a
                    // time
                    // instead of requiring all-or-none semantics for all the tasks we think we
                    // listed,
                    // or else contention could be very bad.
                    ms.rollback();
                    throw new RetryOnConcurrencyException(
                        "Failed to lease available task with status %s, info: %s",
                        result.getReturnStatus(), result.getExtraInformation());
                  }
                })
            .collect(Collectors.toList());
    return EntitiesResult.fromPage(Page.fromItems(loadedTasks));
  }

  @Override
  public @Nonnull EntitiesResult loadTasks(
      @Nonnull PolarisCallContext callCtx, String executorId, PageToken pageToken) {
    TransactionalPersistence ms = ((TransactionalPersistence) callCtx.getMetaStore());
    return ms.runInTransaction(callCtx, () -> this.loadTasks(callCtx, ms, executorId, pageToken));
  }

  /** {@inheritDoc} */
  @Override
  public @Nonnull ScopedCredentialsResult getSubscopedCredsForEntity(
      @Nonnull PolarisCallContext callCtx,
      long catalogId,
      long entityId,
      PolarisEntityType entityType,
      boolean allowListOperation,
      @Nonnull Set<String> allowedReadLocations,
      @Nonnull Set<String> allowedWriteLocations,
      Optional<String> refreshCredentialsEndpoint) {

    // get meta store session we should be using
    TransactionalPersistence ms = ((TransactionalPersistence) callCtx.getMetaStore());
    getDiagnostics()
        .check(
            !allowedReadLocations.isEmpty() || !allowedWriteLocations.isEmpty(),
            "allowed_locations_to_subscope_is_required");

    // reload the entity, error out if not found
    EntityResult reloadedEntity = loadEntity(callCtx, catalogId, entityId, entityType);
    if (reloadedEntity.getReturnStatus() != BaseResult.ReturnStatus.SUCCESS) {
      return new ScopedCredentialsResult(
          reloadedEntity.getReturnStatus(), reloadedEntity.getExtraInformation());
    }

    // get storage integration
    PolarisStorageIntegration<PolarisStorageConfigurationInfo> storageIntegration =
        ms.loadPolarisStorageIntegrationInCurrentTxn(callCtx, reloadedEntity.getEntity());

    // cannot be null
    getDiagnostics()
        .checkNotNull(
            storageIntegration,
            "storage_integration_not_exists",
            "catalogId={}, entityId={}",
            catalogId,
            entityId);

    try {
      StorageAccessConfig storageAccessConfig =
          storageIntegration.getSubscopedCreds(
              callCtx.getRealmConfig(),
              allowListOperation,
              allowedReadLocations,
              allowedWriteLocations,
              refreshCredentialsEndpoint);
      return new ScopedCredentialsResult(storageAccessConfig);
    } catch (Exception ex) {
      return new ScopedCredentialsResult(
          BaseResult.ReturnStatus.SUBSCOPE_CREDS_ERROR, ex.getMessage());
    }
  }

  /** {@link #loadResolvedEntityById(PolarisCallContext, long, long, PolarisEntityType)} */
  private @Nonnull ResolvedEntityResult loadResolvedEntityById(
      @Nonnull PolarisCallContext callCtx,
      @Nonnull TransactionalPersistence ms,
      long entityCatalogId,
      long entityId,
      int typeCode) {

    // load that entity
    PolarisBaseEntity entity =
        ms.lookupEntityInCurrentTxn(callCtx, entityCatalogId, entityId, typeCode);

    // if entity not found, return null
    if (entity == null) {
      return new ResolvedEntityResult(BaseResult.ReturnStatus.ENTITY_NOT_FOUND, null);
    }

    // load the grant records
    final List<PolarisGrantRecord> grantRecords;
    if (entity.getType().isGrantee()) {
      grantRecords =
          new ArrayList<>(
              ms.loadAllGrantRecordsOnGranteeInCurrentTxn(callCtx, entityCatalogId, entityId));
      grantRecords.addAll(
          ms.loadAllGrantRecordsOnSecurableInCurrentTxn(callCtx, entityCatalogId, entityId));
    } else {
      grantRecords =
          ms.loadAllGrantRecordsOnSecurableInCurrentTxn(callCtx, entityCatalogId, entityId);
    }

    // return the result
    return new ResolvedEntityResult(entity, entity.getGrantRecordsVersion(), grantRecords);
  }

  /** {@inheritDoc} */
  @Override
  public @Nonnull ResolvedEntityResult loadResolvedEntityById(
      @Nonnull PolarisCallContext callCtx,
      long entityCatalogId,
      long entityId,
      PolarisEntityType entityType) {
    // get metastore we should be using
    TransactionalPersistence ms = ((TransactionalPersistence) callCtx.getMetaStore());

    // need to run inside a read transaction
    return ms.runInReadTransaction(
        callCtx,
        () ->
            this.loadResolvedEntityById(
                callCtx, ms, entityCatalogId, entityId, entityType.getCode()));
  }

  /** {@link #loadResolvedEntityById(PolarisCallContext, long, long, PolarisEntityType)} */
  private @Nonnull ResolvedEntityResult loadResolvedEntityByName(
      @Nonnull PolarisCallContext callCtx,
      @Nonnull TransactionalPersistence ms,
      long entityCatalogId,
      long parentId,
      @Nonnull PolarisEntityType entityType,
      @Nonnull String entityName) {

    // load that entity
    PolarisBaseEntity entity =
        ms.lookupEntityByNameInCurrentTxn(
            callCtx, entityCatalogId, parentId, entityType.getCode(), entityName);

    // null if entity not found
    if (entity == null) {
      return new ResolvedEntityResult(BaseResult.ReturnStatus.ENTITY_NOT_FOUND, null);
    }

    // load the grant records
    final List<PolarisGrantRecord> grantRecords;
    if (entity.getType().isGrantee()) {
      grantRecords =
          new ArrayList<>(
              ms.loadAllGrantRecordsOnGranteeInCurrentTxn(
                  callCtx, entityCatalogId, entity.getId()));
      grantRecords.addAll(
          ms.loadAllGrantRecordsOnSecurableInCurrentTxn(callCtx, entityCatalogId, entity.getId()));
    } else {
      grantRecords =
          ms.loadAllGrantRecordsOnSecurableInCurrentTxn(callCtx, entityCatalogId, entity.getId());
    }

    // return the result
    return new ResolvedEntityResult(entity, entity.getGrantRecordsVersion(), grantRecords);
  }

  /** {@inheritDoc} */
  @Override
  public @Nonnull ResolvedEntityResult loadResolvedEntityByName(
      @Nonnull PolarisCallContext callCtx,
      long entityCatalogId,
      long parentId,
      @Nonnull PolarisEntityType entityType,
      @Nonnull String entityName) {
    // get metastore we should be using
    TransactionalPersistence ms = ((TransactionalPersistence) callCtx.getMetaStore());

    // need to run inside a read transaction
    ResolvedEntityResult result =
        ms.runInReadTransaction(
            callCtx,
            () ->
                this.loadResolvedEntityByName(
                    callCtx, ms, entityCatalogId, parentId, entityType, entityName));
    if (PolarisEntityConstants.getRootContainerName().equals(entityName)
        && entityType == PolarisEntityType.ROOT
        && !result.isSuccess()) {
      // Backfill rootContainer if needed.
      ms.runActionInTransaction(
          callCtx,
          () -> {
            PolarisBaseEntity rootContainer =
                new PolarisBaseEntity(
                    PolarisEntityConstants.getNullId(),
                    PolarisEntityConstants.getRootEntityId(),
                    PolarisEntityType.ROOT,
                    PolarisEntitySubType.NULL_SUBTYPE,
                    PolarisEntityConstants.getRootEntityId(),
                    PolarisEntityConstants.getRootContainerName());
            EntityResult backfillResult =
                this.createEntityIfNotExists(callCtx, ms, null, rootContainer);
            if (backfillResult.isSuccess()) {
              PolarisBaseEntity serviceAdminRole =
                  ms.lookupEntityByNameInCurrentTxn(
                      callCtx,
                      0L,
                      0L,
                      PolarisEntityType.PRINCIPAL_ROLE.getCode(),
                      PolarisEntityConstants.getNameOfPrincipalServiceAdminRole());
              if (serviceAdminRole != null) {
                this.persistNewGrantRecord(
                    callCtx,
                    ms,
                    rootContainer,
                    serviceAdminRole,
                    PolarisPrivilege.SERVICE_MANAGE_ACCESS);
              }
            }
          });

      // Redo the lookup in a separate read transaction.
      result =
          ms.runInReadTransaction(
              callCtx,
              () ->
                  this.loadResolvedEntityByName(
                      callCtx, ms, entityCatalogId, parentId, entityType, entityName));
    }
    return result;
  }

  /** {@inheritDoc} */
  private @Nonnull ResolvedEntityResult refreshResolvedEntity(
      @Nonnull PolarisCallContext callCtx,
      @Nonnull TransactionalPersistence ms,
      int entityVersion,
      int entityGrantRecordsVersion,
      @Nonnull PolarisEntityType entityType,
      long entityCatalogId,
      long entityId) {

    // load version information
    PolarisChangeTrackingVersions entityVersions =
        ms.lookupEntityVersionsInCurrentTxn(
                callCtx, List.of(new PolarisEntityId(entityCatalogId, entityId)))
            .get(0);

    // if null, the entity has been purged
    if (entityVersions == null) {
      return new ResolvedEntityResult(BaseResult.ReturnStatus.ENTITY_NOT_FOUND, null);
    }

    // load the entity if something changed
    final PolarisBaseEntity entity;
    if (entityVersion != entityVersions.getEntityVersion()) {
      entity =
          ms.lookupEntityInCurrentTxn(callCtx, entityCatalogId, entityId, entityType.getCode());

      // if not found, return null
      if (entity == null) {
        return new ResolvedEntityResult(BaseResult.ReturnStatus.ENTITY_NOT_FOUND, null);
      }
    } else {
      // entity has not changed, no need to reload it
      entity = null;
    }

    // load the grant records if required
    final List<PolarisGrantRecord> grantRecords;
    if (entityVersions.getGrantRecordsVersion() != entityGrantRecordsVersion) {
      if (entityType.isGrantee()) {
        grantRecords =
            new ArrayList<>(
                ms.loadAllGrantRecordsOnGranteeInCurrentTxn(callCtx, entityCatalogId, entityId));
        grantRecords.addAll(
            ms.loadAllGrantRecordsOnSecurableInCurrentTxn(callCtx, entityCatalogId, entityId));
      } else {
        grantRecords =
            ms.loadAllGrantRecordsOnSecurableInCurrentTxn(callCtx, entityCatalogId, entityId);
      }
    } else {
      grantRecords = null;
    }

    // return the result
    return new ResolvedEntityResult(entity, entityVersions.getGrantRecordsVersion(), grantRecords);
  }

  /** {@inheritDoc} */
  @Override
  public @Nonnull ResolvedEntityResult refreshResolvedEntity(
      @Nonnull PolarisCallContext callCtx,
      int entityVersion,
      int entityGrantRecordsVersion,
      @Nonnull PolarisEntityType entityType,
      long entityCatalogId,
      long entityId) {
    // get metastore we should be using
    TransactionalPersistence ms = ((TransactionalPersistence) callCtx.getMetaStore());

    // need to run inside a read transaction
    return ms.runInReadTransaction(
        callCtx,
        () ->
            this.refreshResolvedEntity(
                callCtx,
                ms,
                entityVersion,
                entityGrantRecordsVersion,
                entityType,
                entityCatalogId,
                entityId));
  }

  /** {@inheritDoc} */
  @Override
  public <T extends PolarisEntity & LocationBasedEntity>
      Optional<Optional<String>> hasOverlappingSiblings(
          @Nonnull PolarisCallContext callContext, T entity) {
    TransactionalPersistence ms = ((TransactionalPersistence) callContext.getMetaStore());
    return ms.runInTransaction(
        callContext,
        () -> {
          return callContext.getMetaStore().hasOverlappingSiblings(callContext, entity);
        });
  }

  /** {@inheritDoc} */
  @Override
  public @Nonnull PolicyAttachmentResult attachPolicyToEntity(
      @Nonnull PolarisCallContext callCtx,
      @Nonnull List<PolarisEntityCore> targetCatalogPath,
      @Nonnull PolarisEntityCore target,
      @Nonnull List<PolarisEntityCore> policyCatalogPath,
      @Nonnull PolicyEntity policy,
      Map<String, String> parameters) {
    // get metastore we should be using
    TransactionalPersistence ms = ((TransactionalPersistence) callCtx.getMetaStore());

    return ms.runInTransaction(
        callCtx,
        () ->
            this.doAttachPolicyToEntity(
                callCtx, ms, targetCatalogPath, target, policyCatalogPath, policy, parameters));
  }

  /**
   * See {@link #attachPolicyToEntity(PolarisCallContext, List, PolarisEntityCore, List,
   * PolicyEntity, Map)}
   */
  private @Nonnull PolicyAttachmentResult doAttachPolicyToEntity(
      @Nonnull PolarisCallContext callCtx,
      @Nonnull TransactionalPersistence ms,
      @Nonnull List<PolarisEntityCore> targetCatalogPath,
      @Nonnull PolarisEntityCore target,
      @Nonnull List<PolarisEntityCore> policyCatalogPath,
      @Nonnull PolicyEntity policy,
      Map<String, String> parameters) {
    PolarisEntityResolver targetResolver =
        new PolarisEntityResolver(getDiagnostics(), callCtx, ms, targetCatalogPath, target);
    PolarisEntityResolver policyResolver =
        new PolarisEntityResolver(getDiagnostics(), callCtx, ms, policyCatalogPath, policy);
    if (targetResolver.isFailure() || policyResolver.isFailure()) {
      return new PolicyAttachmentResult(BaseResult.ReturnStatus.ENTITY_CANNOT_BE_RESOLVED, null);
    }

    return this.persistNewPolicyMappingRecord(callCtx, ms, target, policy, parameters);
  }

  /** {@inheritDoc} */
  @Override
  public @Nonnull PolicyAttachmentResult detachPolicyFromEntity(
      @Nonnull PolarisCallContext callCtx,
      @Nonnull List<PolarisEntityCore> targetCatalogPath,
      @Nonnull PolarisEntityCore target,
      @Nonnull List<PolarisEntityCore> policyCatalogPath,
      @Nonnull PolicyEntity policy) {
    TransactionalPersistence ms = ((TransactionalPersistence) callCtx.getMetaStore());
    return ms.runInTransaction(
        callCtx,
        () ->
            this.doDetachPolicyFromEntity(
                callCtx, ms, targetCatalogPath, target, policyCatalogPath, policy));
  }

  /**
   * See {@link #detachPolicyFromEntity(PolarisCallContext, List, PolarisEntityCore,
   * List,PolicyEntity)}
   */
  private PolicyAttachmentResult doDetachPolicyFromEntity(
      @Nonnull PolarisCallContext callCtx,
      @Nonnull TransactionalPersistence ms,
      @Nonnull List<PolarisEntityCore> targetCatalogPath,
      @Nonnull PolarisEntityCore target,
      @Nonnull List<PolarisEntityCore> policyCatalogPath,
      @Nonnull PolicyEntity policy) {
    PolarisEntityResolver targetResolver =
        new PolarisEntityResolver(getDiagnostics(), callCtx, ms, targetCatalogPath, target);
    PolarisEntityResolver policyResolver =
        new PolarisEntityResolver(getDiagnostics(), callCtx, ms, policyCatalogPath, policy);
    if (targetResolver.isFailure() || policyResolver.isFailure()) {
      return new PolicyAttachmentResult(BaseResult.ReturnStatus.ENTITY_CANNOT_BE_RESOLVED, null);
    }

    PolarisPolicyMappingRecord mappingRecord =
        ms.lookupPolicyMappingRecordInCurrentTxn(
            callCtx,
            target.getCatalogId(),
            target.getId(),
            policy.getPolicyTypeCode(),
            policy.getCatalogId(),
            policy.getId());
    if (mappingRecord == null) {
      return new PolicyAttachmentResult(BaseResult.ReturnStatus.POLICY_MAPPING_NOT_FOUND, null);
    }

    ms.deleteFromPolicyMappingRecordsInCurrentTxn(callCtx, mappingRecord);

    return new PolicyAttachmentResult(mappingRecord);
  }

  /** {@inheritDoc} */
  @Override
  public @Nonnull LoadPolicyMappingsResult loadPoliciesOnEntity(
      @Nonnull PolarisCallContext callCtx, @Nonnull PolarisEntityCore target) {
    TransactionalPersistence ms = ((TransactionalPersistence) callCtx.getMetaStore());
    return ms.runInReadTransaction(callCtx, () -> this.doLoadPoliciesOnEntity(callCtx, ms, target));
  }

  /** See {@link #loadPoliciesOnEntity(PolarisCallContext, PolarisEntityCore)} */
  private LoadPolicyMappingsResult doLoadPoliciesOnEntity(
      @Nonnull PolarisCallContext callCtx,
      @Nonnull TransactionalPersistence ms,
      @Nonnull PolarisEntityCore target) {
    PolarisBaseEntity entity =
        ms.lookupEntityInCurrentTxn(
            callCtx, target.getCatalogId(), target.getId(), target.getTypeCode());
    if (entity == null) {
      // Target entity does not exists
      return new LoadPolicyMappingsResult(BaseResult.ReturnStatus.ENTITY_NOT_FOUND, null);
    }

    final List<PolarisPolicyMappingRecord> policyMappingRecords =
        ms.loadAllPoliciesOnTargetInCurrentTxn(callCtx, target.getCatalogId(), target.getId());

    List<PolarisBaseEntity> policyEntities =
        loadPoliciesFromMappingRecords(callCtx, ms, policyMappingRecords);
    return new LoadPolicyMappingsResult(policyMappingRecords, policyEntities);
  }

  /** {@inheritDoc} */
  @Override
  public @Nonnull LoadPolicyMappingsResult loadPoliciesOnEntityByType(
      @Nonnull PolarisCallContext callCtx,
      @Nonnull PolarisEntityCore target,
      @Nonnull PolicyType policyType) {
    TransactionalPersistence ms = ((TransactionalPersistence) callCtx.getMetaStore());
    return ms.runInReadTransaction(
        callCtx, () -> this.doLoadPoliciesOnEntityByType(callCtx, ms, target, policyType));
  }

  /** See {@link #loadPoliciesOnEntityByType(PolarisCallContext, PolarisEntityCore, PolicyType)} */
  private LoadPolicyMappingsResult doLoadPoliciesOnEntityByType(
      @Nonnull PolarisCallContext callCtx,
      @Nonnull TransactionalPersistence ms,
      @Nonnull PolarisEntityCore target,
      @Nonnull PolicyType policyType) {
    PolarisBaseEntity entity =
        ms.lookupEntityInCurrentTxn(
            callCtx, target.getCatalogId(), target.getId(), target.getTypeCode());
    if (entity == null) {
      // Target entity does not exists
      return new LoadPolicyMappingsResult(BaseResult.ReturnStatus.ENTITY_NOT_FOUND, null);
    }

    final List<PolarisPolicyMappingRecord> policyMappingRecords =
        ms.loadPoliciesOnTargetByTypeInCurrentTxn(
            callCtx, target.getCatalogId(), target.getId(), policyType.getCode());
    List<PolarisBaseEntity> policyEntities =
        loadPoliciesFromMappingRecords(callCtx, ms, policyMappingRecords);
    return new LoadPolicyMappingsResult(policyMappingRecords, policyEntities);
  }

  /**
   * Create and persist a new policy mapping record
   *
   * @param callCtx call context
   * @param ms meta store in read/write mode
   * @param target target
   * @param policy policy
   * @param parameters optional parameters
   * @return new policy mapping record which was created and persisted
   */
  private @Nonnull PolicyAttachmentResult persistNewPolicyMappingRecord(
      @Nonnull PolarisCallContext callCtx,
      @Nonnull TransactionalPersistence ms,
      @Nonnull PolarisEntityCore target,
      @Nonnull PolicyEntity policy,
      Map<String, String> parameters) {
    getDiagnostics().checkNotNull(target, "unexpected_null_target");
    getDiagnostics().checkNotNull(policy, "unexpected_null_policy");

    PolarisPolicyMappingRecord mappingRecord =
        new PolarisPolicyMappingRecord(
            target.getCatalogId(),
            target.getId(),
            policy.getCatalogId(),
            policy.getId(),
            policy.getPolicyTypeCode(),
            parameters);

    try {
      ms.checkConditionsForWriteToPolicyMappingRecordsInCurrentTxn(callCtx, mappingRecord);
      ms.writeToPolicyMappingRecordsInCurrentTxn(callCtx, mappingRecord);
    } catch (IllegalArgumentException e) {
      return new PolicyAttachmentResult(
          BaseResult.ReturnStatus.UNEXPECTED_ERROR_SIGNALED, "Unknown policy type");
    } catch (PolicyMappingAlreadyExistsException e) {
      return new PolicyAttachmentResult(
          BaseResult.ReturnStatus.POLICY_MAPPING_OF_SAME_TYPE_ALREADY_EXISTS,
          e.getExistingRecord().getPolicyTypeCode());
    }

    return new PolicyAttachmentResult(mappingRecord);
  }

  /**
   * Load policies from a list of policy mapping records
   *
   * @param callCtx call context
   * @param ms meta store
   * @param policyMappingRecords a list of policy mapping records
   * @return a list of policy entities
   */
  private List<PolarisBaseEntity> loadPoliciesFromMappingRecords(
      @Nonnull PolarisCallContext callCtx,
      @Nonnull TransactionalPersistence ms,
      @Nonnull List<PolarisPolicyMappingRecord> policyMappingRecords) {
    List<PolarisEntityId> policyEntityIds =
        policyMappingRecords.stream()
            .map(
                policyMappingRecord ->
                    new PolarisEntityId(
                        policyMappingRecord.getPolicyCatalogId(),
                        policyMappingRecord.getPolicyId()))
            .distinct()
            .collect(Collectors.toList());
    return ms.lookupEntitiesInCurrentTxn(callCtx, policyEntityIds);
  }
}
