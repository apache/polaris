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
import java.util.concurrent.atomic.AtomicInteger;
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
 * Implementation of PolarisMetaStoreManager which only relies on one-shot atomic operations into a
 * BasePersistence implementation without any kind of open-ended multi-statement transactions.
 */
public class AtomicOperationMetaStoreManager extends BaseMetaStoreManager {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(AtomicOperationMetaStoreManager.class);

  private final Clock clock;

  public AtomicOperationMetaStoreManager(Clock clock, PolarisDiagnostics diagnostics) {
    super(diagnostics);
    this.clock = clock;
  }

  /**
   * Persist the specified new entity.
   *
   * @param callCtx call context
   * @param ms meta store in read/write mode
   * @param entity entity we need a new persisted record for
   */
  private EntityResult persistNewEntity(
      @Nonnull PolarisCallContext callCtx,
      @Nonnull BasePersistence ms,
      @Nonnull PolarisBaseEntity entity) {
    // Invoke shared logic for validation and filling out remaining fields.
    entity = prepareToPersistNewEntity(callCtx, ms, entity);

    try {
      // write it
      ms.writeEntity(callCtx, entity, true, null);
    } catch (EntityAlreadyExistsException e) {
      if (e.getExistingEntity().getId() == entity.getId()) {
        // Since ids are unique and reserved when generated, a matching id means a low-level retry
        // which should behave idempotently; simply return the entity we were trying to write.
        // Note: We return the "entity" instead of the "existingEntity" in case the entity has
        // also since been updated; our idempotent creation is perfectly allowed to finish with
        // a later timestamp than some update that may have inserted itself after creation but
        // before the retry, and so the caller of the creation should see the original entity we
        // were trying to create, not the updated one.
        return new EntityResult(entity);
      } else {
        // If ids don't match it means we hit a name collision.
        return new EntityResult(
            BaseResult.ReturnStatus.ENTITY_ALREADY_EXISTS, e.getExistingEntity().getSubTypeCode());
      }
    }
    return new EntityResult(entity);
  }

  /**
   * Persist the specified entity after it has been changed. We will update the last changed time,
   * increment the entity version and persist it in one atomic operation.
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
      @Nonnull BasePersistence ms,
      @Nonnull PolarisBaseEntity entity,
      boolean nameOrParentChanged,
      @Nonnull PolarisBaseEntity originalEntity) {
    // Invoke shared logic for validation and updating expected fields.
    entity =
        prepareToPersistEntityAfterChange(callCtx, ms, entity, nameOrParentChanged, originalEntity);

    // persist it to the various slices
    ms.writeEntity(callCtx, entity, nameOrParentChanged, originalEntity);

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
      @Nonnull BasePersistence ms,
      @Nonnull PolarisBaseEntity entity) {

    // validate the entity type and subtype
    getDiagnostics().checkNotNull(entity, "unexpected_null_dpo");
    getDiagnostics().checkNotNull(entity.getName(), "unexpected_null_name");

    // creation timestamp must be filled
    getDiagnostics().check(entity.getDropTimestamp() == 0, "already_dropped");

    // Remove the main entity itself first-thing; once its id no longer resolves successfully
    // it will be pruned out of any grant-record lookups anyways.
    ms.deleteEntity(callCtx, entity);

    // Best-effort cleanup - drop grant records, update grantRecordVersions for affected
    // other entities.
    // TODO: Support some more formal garbage-collection mechanism so that a garbage-collector
    // doesn't have to "guess" at whether orphaned id links are garbage vs "newly allocated" ids
    // that just haven't been fully committed yet. Potentially pre-populate a tombstone
    // record that only behaves as a tombstone if the main entity is also verified to be
    // gone; then garbage-collection looks for cleanup grants based on the tombstone and only
    // removes the tombstone once cleanup is done; cleanup can occur incrementally.

    // delete ALL grant records to (if the entity is a grantee) and from that entity
    final List<PolarisGrantRecord> grantsOnGrantee =
        (entity.getType().isGrantee())
            ? ms.loadAllGrantRecordsOnGrantee(callCtx, entity.getCatalogId(), entity.getId())
            : List.of();
    final List<PolarisGrantRecord> grantsOnSecurable =
        ms.loadAllGrantRecordsOnSecurable(callCtx, entity.getCatalogId(), entity.getId());
    ms.deleteAllEntityGrantRecords(callCtx, entity, grantsOnGrantee, grantsOnSecurable);

    if (entity.getType() == PolarisEntityType.POLICY
        || PolicyMappingUtil.isValidTargetEntityType(entity.getType(), entity.getSubType())) {
      // Best-effort cleanup - for policy and potential target entities, drop all policy mapping
      // records related
      // TODO: Support some more formal garbage-collection mechanism similar to grant records case
      // above
      try {
        final List<PolarisPolicyMappingRecord> mappingOnPolicy =
            (entity.getType() == PolarisEntityType.POLICY)
                ? ms.loadAllTargetsOnPolicy(
                    callCtx,
                    entity.getCatalogId(),
                    entity.getId(),
                    PolicyEntity.of(entity).getPolicyTypeCode())
                : List.of();
        final List<PolarisPolicyMappingRecord> mappingOnTarget =
            (entity.getType() == PolarisEntityType.POLICY)
                ? List.of()
                : ms.loadAllPoliciesOnTarget(callCtx, entity.getCatalogId(), entity.getId());
        ms.deleteAllEntityPolicyMappingRecords(callCtx, entity, mappingOnTarget, mappingOnPolicy);
      } catch (UnsupportedOperationException e) {
        // Policy mapping persistence not implemented, but we should not block dropping entities
      }
    }

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
        ms.lookupEntities(callCtx, new ArrayList<>(entityIdsGrantChanged));
    for (PolarisBaseEntity originalEntity : entities) {
      PolarisBaseEntity entityGrantChanged =
          originalEntity.withGrantRecordsVersion(originalEntity.getGrantRecordsVersion() + 1);
      ms.writeEntity(callCtx, entityGrantChanged, false, originalEntity);
    }

    // if it is a principal, we also need to drop the secrets
    if (entity.getType() == PolarisEntityType.PRINCIPAL) {
      PrincipalEntity principalEntity = PrincipalEntity.of(entity);
      String clientId = principalEntity.getClientId();
      // delete it from the secret slice
      ((IntegrationPersistence) ms).deletePrincipalSecrets(callCtx, clientId, entity.getId());
    }
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
      @Nonnull BasePersistence ms,
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
    ms.writeToGrantRecords(callCtx, grantRecord);

    // load the grantee (either a catalog/principal role or a principal) and increment its grants
    // version
    PolarisBaseEntity granteeEntity =
        ms.lookupEntity(callCtx, grantee.getCatalogId(), grantee.getId(), grantee.getTypeCode());
    getDiagnostics().checkNotNull(granteeEntity, "grantee_not_found", "grantee={}", grantee);
    // grants have changed, we need to bump-up the grants version
    PolarisBaseEntity updatedGranteeEntity =
        granteeEntity.withGrantRecordsVersion(granteeEntity.getGrantRecordsVersion() + 1);
    ms.writeEntity(callCtx, updatedGranteeEntity, false, granteeEntity);

    // we also need to invalidate the grants on that securable so that we can reload them.
    // load the securable and increment its grants version
    PolarisBaseEntity securableEntity =
        ms.lookupEntity(
            callCtx, securable.getCatalogId(), securable.getId(), securable.getTypeCode());
    getDiagnostics()
        .checkNotNull(securableEntity, "securable_not_found", "securable={}", securable);
    // grants have changed, we need to bump-up the grants version
    PolarisBaseEntity updatedSecurableEntity =
        securableEntity.withGrantRecordsVersion(securableEntity.getGrantRecordsVersion() + 1);
    ms.writeEntity(callCtx, updatedSecurableEntity, false, securableEntity);

    // TODO: Reorder and/or expose bulk update of both grantRecordsVersions and grant records. In
    // the meantime, cache can be disabled or configured with a short enough expiry time to
    // define an "eventual consistency" timeframe.
    // TODO: Figure out if it's actually necessary to separately validate whether the entities have
    // not changed, if we plan to include the compare-and-swap in the helper method that updates the
    // grantRecordsVersions already.

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
      @Nonnull BasePersistence ms,
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
    ms.deleteFromGrantRecords(callCtx, grantRecord);

    // load the grantee and increment its grants version
    PolarisBaseEntity refreshGrantee =
        ms.lookupEntity(callCtx, grantee.getCatalogId(), grantee.getId(), grantee.getTypeCode());
    getDiagnostics()
        .checkNotNull(
            refreshGrantee, "missing_grantee", "grantRecord={} grantee={}", grantRecord, grantee);
    // grants have changed, we need to bump-up the grants version
    PolarisBaseEntity updatedRefreshGrantee =
        refreshGrantee.withGrantRecordsVersion(refreshGrantee.getGrantRecordsVersion() + 1);
    ms.writeEntity(callCtx, updatedRefreshGrantee, false, refreshGrantee);

    // we also need to invalidate the grants on that securable so that we can reload them.
    // load the securable and increment its grants version
    PolarisBaseEntity refreshSecurable =
        ms.lookupEntity(
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
    ms.writeEntity(callCtx, updatedRefreshSecurable, false, refreshSecurable);

    // TODO: Reorder and/or expose bulk update of both grantRecordsVersions and grant records. In
    // the meantime, cache can be disabled or configured with a short enough expiry time to
    // define an "eventual consistency" timeframe.
  }

  /** {@inheritDoc} */
  @Override
  public @Nonnull CreateCatalogResult createCatalog(
      @Nonnull PolarisCallContext callCtx,
      @Nonnull PolarisBaseEntity catalog,
      @Nonnull List<PolarisEntityCore> principalRoles) {
    // get metastore we should be using
    BasePersistence ms = callCtx.getMetaStore();

    // validate input
    getDiagnostics().checkNotNull(catalog, "unexpected_null_catalog");

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
          ((IntegrationPersistence) ms)
              .createStorageIntegration(
                  callCtx,
                  catalog.getCatalogId(),
                  catalog.getId(),
                  PolarisStorageConfigurationInfo.deserialize(storageConfigInfoStr));
    } else {
      integration = null;
    }

    // check if that catalog has already been created
    // This can be done safely as a separate atomic operation before trying to create the catalog
    // because same-id idempotent-retry collisions of this sort are necessarily sequential, so
    // there is no concurrency conflict for something else creating a catalog of this same id.
    PolarisBaseEntity refreshCatalog =
        ms.lookupEntity(callCtx, catalog.getCatalogId(), catalog.getId(), catalog.getTypeCode());

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
          ms.lookupEntityByName(
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
    ((IntegrationPersistence) ms).persistStorageIntegrationIfNeeded(callCtx, catalog, integration);

    // now create and persist new catalog entity
    EntityResult lowLevelResult = this.persistNewEntity(callCtx, ms, catalog);
    if (lowLevelResult.getReturnStatus() == BaseResult.ReturnStatus.ENTITY_ALREADY_EXISTS) {
      // TODO: Garbage-collection should include integrations, and anything else created before
      // this if the server crashes before being able to do this cleanup.
      // TODO: Perform best-effort cleanup. For now, none of the codebase apparently cleans
      // up storage integrations "if needed", but also the default impls don't create any
      // storage integrations "if needed" either.
      return new CreateCatalogResult(BaseResult.ReturnStatus.ENTITY_ALREADY_EXISTS, null);
    }

    // create the catalog admin role for this new catalog
    long adminRoleId = ms.generateNewId(callCtx);
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
          ms.lookupEntityByName(
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

    // TODO: Reorder and/or expose bulk update/create of new entities and grant records. In the
    // meantime, if a server crashes halfway through catalog creation, it might be in a partially
    // initialized state. In such a case, the correct action is to simply delete the catalog
    // and recreate it -- SERVICE_MANAGE_ACCESS already possesses CATALOG_DROP at the
    // root-container level even if it requires the grants in here to have CATALOG_MANAGE_ACCESS
    // or CATALOG_MANAGE_METADATA, so no one can use the catalog if this initialization is
    // incomplete, but the won't be "stuck" orphaned since the service admin can still delete the
    // catalog.

    // success, return the two entities
    return new CreateCatalogResult(catalog, adminRole);
  }

  /** {@inheritDoc} */
  @Override
  public @Nonnull BaseResult bootstrapPolarisService(@Nonnull PolarisCallContext callCtx) {
    // get meta store we should be using
    BasePersistence ms = callCtx.getMetaStore();

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
    long rootPrincipalId = ms.generateNewId(callCtx);
    PrincipalEntity rootPrincipal =
        new PrincipalEntity.Builder()
            .setId(rootPrincipalId)
            .setName(PolarisEntityConstants.getRootPrincipalName())
            .setCreateTimestamp(System.currentTimeMillis())
            .build();
    this.createPrincipal(callCtx, rootPrincipal);

    // now create the account admin principal role
    long serviceAdminPrincipalRoleId = ms.generateNewId(callCtx);
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

    // TODO: Make idempotent by being able to continue where it left off for the context's realm.
    // In the meantime, if a realm was only partially initialized before the server crashed,
    // it's fine to purge the realm and retry the bootstrap.

    // all good
    return new BaseResult(BaseResult.ReturnStatus.SUCCESS);
  }

  @Override
  public @Nonnull BaseResult purge(@Nonnull PolarisCallContext callCtx) {
    // get meta store we should be using
    BasePersistence ms = callCtx.getMetaStore();

    LOGGER.warn("Deleting all metadata in the metastore...");
    ms.deleteAll(callCtx);
    LOGGER.warn("Finished deleting all metadata in the metastore");

    // all good
    return new BaseResult(BaseResult.ReturnStatus.SUCCESS);
  }

  /** {@inheritDoc} */
  @Override
  public @Nonnull EntityResult readEntityByName(
      @Nonnull PolarisCallContext callCtx,
      @Nullable List<PolarisEntityCore> catalogPath,
      @Nonnull PolarisEntityType entityType,
      @Nonnull PolarisEntitySubType entitySubType,
      @Nonnull String name) {
    BasePersistence ms = callCtx.getMetaStore();

    // now looking the entity by name
    // TODO: Clean up shared logic for catalogId/parentId
    long catalogId =
        catalogPath == null || catalogPath.size() == 0 ? 0l : catalogPath.get(0).getId();
    long parentId =
        catalogPath == null || catalogPath.size() == 0
            ? 0l
            : catalogPath.get(catalogPath.size() - 1).getId();
    PolarisBaseEntity entity =
        ms.lookupEntityByName(callCtx, catalogId, parentId, entityType.getCode(), name);

    // if found, check if subType really matches
    if (entity != null
        && entitySubType != PolarisEntitySubType.ANY_SUBTYPE
        && entity.getSubTypeCode() != entitySubType.getCode()) {
      entity = null;
    }

    // TODO: Use post-validation to enforce consistent view against catalogPath. In the
    // meantime, happens-before ordering semantics aren't guaranteed during high-concurrency
    // race conditions, such as first revoking a grant on a namespace before adding sensitive
    // data to a table; but the window of inconsistency is only the duration of a single
    // in-flight request (the cache-based resolution follows a different path entirely).

    // success, return what we found
    return (entity == null)
        ? new EntityResult(BaseResult.ReturnStatus.ENTITY_NOT_FOUND, null)
        : new EntityResult(entity);
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
    BasePersistence ms = callCtx.getMetaStore();

    // return list of active entities
    // TODO: Clean up shared logic for catalogId/parentId
    long catalogId = catalogPath == null || catalogPath.isEmpty() ? 0L : catalogPath.get(0).getId();
    long parentId =
        catalogPath == null || catalogPath.isEmpty()
            ? 0L
            : catalogPath.get(catalogPath.size() - 1).getId();

    Page<EntityNameLookupRecord> resultPage =
        ms.listEntities(callCtx, catalogId, parentId, entityType, entitySubType, pageToken);

    // TODO: Use post-validation to enforce consistent view against catalogPath. In the
    // meantime, happens-before ordering semantics aren't guaranteed during high-concurrency
    // race conditions, such as first revoking a grant on a namespace before adding a table
    // with sensitive data; but the window of inconsistency is only the duration of a single
    // in-flight request (the cache-based resolution follows a different path entirely).

    return ListEntitiesResult.fromPage(resultPage);
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
    BasePersistence ms = callCtx.getMetaStore();

    // return list of active entities
    // TODO: Clean up shared logic for catalogId/parentId
    long catalogId = catalogPath == null || catalogPath.isEmpty() ? 0L : catalogPath.get(0).getId();
    long parentId =
        catalogPath == null || catalogPath.isEmpty()
            ? 0L
            : catalogPath.get(catalogPath.size() - 1).getId();

    // TODO: Use post-validation to enforce consistent view against catalogPath. In the
    // meantime, happens-before ordering semantics aren't guaranteed during high-concurrency
    // race conditions, such as first revoking a grant on a namespace before adding a table
    // with sensitive data; but the window of inconsistency is only the duration of a single
    // in-flight request (the cache-based resolution follows a different path entirely).

    return ms.loadEntities(
        callCtx,
        catalogId,
        parentId,
        entityType,
        entitySubType,
        entity -> true,
        Function.identity(),
        pageToken);
  }

  /** {@inheritDoc} */
  @Override
  public @Nonnull CreatePrincipalResult createPrincipal(
      @Nonnull PolarisCallContext callCtx, @Nonnull PrincipalEntity principal) {
    // get metastore we should be using
    BasePersistence ms = callCtx.getMetaStore();

    // validate input
    getDiagnostics().checkNotNull(principal, "unexpected_null_principal");

    // check if that catalog has already been created
    PrincipalEntity refreshPrincipal =
        PrincipalEntity.of(
            ms.lookupEntity(
                callCtx, principal.getCatalogId(), principal.getId(), principal.getTypeCode()));

    // if found, probably a retry, simply return the previously created principal
    // This can be done safely as a separate atomic operation before trying to create the principal
    // because same-id idempotent-retry collisions of this sort are necessarily sequential, so
    // there is no concurrency conflict for something else creating a principal of this same id.
    if (refreshPrincipal != null) {
      String clientId = refreshPrincipal.getClientId();
      getDiagnostics().checkNotNull(clientId, "null_client_id", "principal={}", refreshPrincipal);
      getDiagnostics()
          .check(!clientId.isEmpty(), "empty_client_id", "principal={}", refreshPrincipal);

      // get the main and secondary secrets for that client
      PolarisPrincipalSecrets principalSecrets =
          ((IntegrationPersistence) ms).loadPrincipalSecrets(callCtx, clientId);

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

    // generate new secrets for this principal
    PolarisPrincipalSecrets principalSecrets =
        ((IntegrationPersistence) ms)
            .generateNewPrincipalSecrets(callCtx, principal.getName(), principal.getId());

    // remember client id
    PrincipalEntity updatedPrincipal =
        new PrincipalEntity.Builder(principal)
            .setClientId(principalSecrets.getPrincipalClientId())
            .build();
    // now create and persist new catalog entity
    EntityResult lowLevelResult = this.persistNewEntity(callCtx, ms, updatedPrincipal);
    if (lowLevelResult.getReturnStatus() == BaseResult.ReturnStatus.ENTITY_ALREADY_EXISTS) {
      // Name collision; do best-effort cleanup of the new principalSecrets we created
      // TODO: Garbage-collection should include principal secrets if the server crashes
      // before being able to do this cleanup.
      ((IntegrationPersistence) ms)
          .deletePrincipalSecrets(
              callCtx, principalSecrets.getPrincipalClientId(), updatedPrincipal.getId());
      return new CreatePrincipalResult(BaseResult.ReturnStatus.ENTITY_ALREADY_EXISTS, null);
    }

    // success, return the two entities
    return new CreatePrincipalResult(updatedPrincipal, principalSecrets);
  }

  /** {@inheritDoc} */
  @Override
  public @Nonnull PrincipalSecretsResult loadPrincipalSecrets(
      @Nonnull PolarisCallContext callCtx, @Nonnull String clientId) {
    // get metastore we should be using
    BasePersistence ms = callCtx.getMetaStore();

    PolarisPrincipalSecrets secrets =
        ((IntegrationPersistence) ms).loadPrincipalSecrets(callCtx, clientId);

    return (secrets == null)
        ? new PrincipalSecretsResult(BaseResult.ReturnStatus.ENTITY_NOT_FOUND, null)
        : new PrincipalSecretsResult(secrets);
  }

  /** {@inheritDoc} */
  @Override
  public @Nonnull void deletePrincipalSecrets(
      @Nonnull PolarisCallContext callCtx, @Nonnull String clientId, long principalId) {
    // get metastore we should be using
    BasePersistence ms = callCtx.getMetaStore();
    ((IntegrationPersistence) ms).deletePrincipalSecrets(callCtx, clientId, principalId);
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
    BasePersistence ms = callCtx.getMetaStore();

    // if not found, the principal must have been dropped
    Optional<PrincipalEntity> principalLookup = findPrincipalById(callCtx, principalId);
    if (principalLookup.isEmpty()) {
      return new PrincipalSecretsResult(BaseResult.ReturnStatus.ENTITY_NOT_FOUND, null);
    }

    PrincipalEntity principal = principalLookup.get();
    Map<String, String> internalProps = principal.getInternalPropertiesAsMap();

    boolean doReset =
        reset
            || internalProps.get(
                    PolarisEntityConstants.PRINCIPAL_CREDENTIAL_ROTATION_REQUIRED_STATE)
                != null;
    PolarisPrincipalSecrets secrets =
        ((IntegrationPersistence) ms)
            .rotatePrincipalSecrets(callCtx, clientId, principalId, doReset, oldSecretHash);

    PolarisBaseEntity.Builder principalBuilder = new PolarisBaseEntity.Builder(principal);
    if (reset
        && !internalProps.containsKey(
            PolarisEntityConstants.PRINCIPAL_CREDENTIAL_ROTATION_REQUIRED_STATE)) {
      internalProps.put(
          PolarisEntityConstants.PRINCIPAL_CREDENTIAL_ROTATION_REQUIRED_STATE, "true");
      principalBuilder.internalPropertiesAsMap(internalProps);
      principalBuilder.entityVersion(principal.getEntityVersion() + 1);
      ms.writeEntity(callCtx, principalBuilder.build(), true, principal);
    } else if (internalProps.containsKey(
        PolarisEntityConstants.PRINCIPAL_CREDENTIAL_ROTATION_REQUIRED_STATE)) {
      internalProps.remove(PolarisEntityConstants.PRINCIPAL_CREDENTIAL_ROTATION_REQUIRED_STATE);
      principalBuilder.internalPropertiesAsMap(internalProps);
      principalBuilder.entityVersion(principal.getEntityVersion() + 1);
      ms.writeEntity(callCtx, principalBuilder.build(), true, principal);
    }

    // TODO: Rethink the atomicity of the relationship between principalSecrets and principal

    return (secrets == null)
        ? new PrincipalSecretsResult(BaseResult.ReturnStatus.ENTITY_NOT_FOUND, null)
        : new PrincipalSecretsResult(secrets);
  }

  @Override
  public @Nonnull PrincipalSecretsResult resetPrincipalSecrets(
      @Nonnull PolarisCallContext callCtx,
      long principalId,
      @Nonnull String resolvedClientId,
      String customClientSecret) {
    // get metastore we should be using
    BasePersistence ms = callCtx.getMetaStore();

    // if not found, the principal must have been dropped
    Optional<PrincipalEntity> principalEntity = findPrincipalById(callCtx, principalId);
    if (principalEntity.isEmpty()) {
      return new PrincipalSecretsResult(BaseResult.ReturnStatus.ENTITY_NOT_FOUND, null);
    }

    PolarisPrincipalSecrets secrets =
        ((IntegrationPersistence) ms)
            .storePrincipalSecrets(callCtx, principalId, resolvedClientId, customClientSecret);
    return (secrets == null)
        ? new PrincipalSecretsResult(BaseResult.ReturnStatus.ENTITY_NOT_FOUND, null)
        : new PrincipalSecretsResult(secrets);
  }

  /** {@inheritDoc} */
  @Override
  public @Nonnull EntityResult createEntityIfNotExists(
      @Nonnull PolarisCallContext callCtx,
      @Nullable List<PolarisEntityCore> catalogPath,
      @Nonnull PolarisBaseEntity entity) {
    // get metastore we should be using
    BasePersistence ms = callCtx.getMetaStore();

    // entity cannot be null
    getDiagnostics().checkNotNull(entity, "unexpected_null_entity");

    // entity name must be specified
    getDiagnostics().checkNotNull(entity.getName(), "unexpected_null_entity_name");

    // TODO: Use post-validation to enforce consistent view against catalogPath. In the
    // meantime, happens-before ordering semantics aren't guaranteed during high-concurrency
    // race conditions, such as first revoking a grant on a namespace before adding sensitive
    // data to a table; but the window of inconsistency is only the duration of a single
    // in-flight request (the cache-based resolution follows a different path entirely).
    return this.persistNewEntity(callCtx, ms, entity);
  }

  @Override
  public @Nonnull EntitiesResult createEntitiesIfNotExist(
      @Nonnull PolarisCallContext callCtx,
      @Nullable List<PolarisEntityCore> catalogPath,
      @Nonnull List<? extends PolarisBaseEntity> entities) {
    // get metastore we should be using
    BasePersistence ms = callCtx.getMetaStore();
    List<PolarisBaseEntity> createdEntities = new ArrayList<>(entities.size());
    for (PolarisBaseEntity entity : entities) {
      PolarisBaseEntity entityToCreate =
          prepareToPersistNewEntity(callCtx, ms, new PolarisBaseEntity.Builder(entity).build());
      createdEntities.add(entityToCreate);
    }

    try {
      ms.writeEntities(callCtx, createdEntities, null);
      // TODO: Use post-validation to enforce consistent view against catalogPath. In the
      // meantime, happens-before ordering semantics aren't guaranteed during high-concurrency
      // race conditions, such as first revoking a grant on a namespace before adding sensitive
      // data to a table; but the window of inconsistency is only the duration of a single
      // in-flight request (the cache-based resolution follows a different path entirely).
    } catch (EntityAlreadyExistsException e) {
      return new EntitiesResult(
          BaseResult.ReturnStatus.ENTITY_ALREADY_EXISTS,
          String.format(
              "Existing entity id: '%s', type %s subtype %s",
              e.getExistingEntity().getId(),
              e.getExistingEntity().getTypeCode(),
              e.getExistingEntity().getSubTypeCode()));
    }

    return new EntitiesResult(Page.fromItems(createdEntities));
  }

  /** {@inheritDoc} */
  @Override
  public @Nonnull EntityResult updateEntityPropertiesIfNotChanged(
      @Nonnull PolarisCallContext callCtx,
      @Nullable List<PolarisEntityCore> catalogPath,
      @Nonnull PolarisBaseEntity entity) {
    // get metastore we should be using
    BasePersistence ms = callCtx.getMetaStore();

    // entity cannot be null
    getDiagnostics().checkNotNull(entity, "unexpected_null_entity");
    // persist this entity after changing it. This will update the version and update the last
    // updated time. Because the entity version is changed, we will update the change tracking table
    try {
      PolarisBaseEntity persistedEntity =
          this.persistEntityAfterChange(callCtx, ms, entity, false, entity);

      // TODO: Revalidate parent-path *after* performing update to fulfill the semantic of returning
      // a NotFoundException if some element of the parent path was concurrently deleted.
      return new EntityResult(persistedEntity);
    } catch (RetryOnConcurrencyException e) {
      return new EntityResult(
          BaseResult.ReturnStatus.TARGET_ENTITY_CONCURRENTLY_MODIFIED, e.getMessage());
    }
  }

  /** {@inheritDoc} */
  @Override
  public @Nonnull EntitiesResult updateEntitiesPropertiesIfNotChanged(
      @Nonnull PolarisCallContext callCtx, @Nonnull List<EntityWithPath> entities) {
    // get metastore we should be using
    BasePersistence ms = callCtx.getMetaStore();

    // ensure that the entities list is not null
    getDiagnostics().checkNotNull(entities, "unexpected_null_entities");

    List<PolarisBaseEntity> updatedEntities = new ArrayList<>(entities.size());
    List<PolarisBaseEntity> originalEntities = new ArrayList<>(entities.size());

    // Collect list of updatedEntities
    for (EntityWithPath entityWithPath : entities) {
      PolarisBaseEntity updatedEntity =
          prepareToPersistEntityAfterChange(
              callCtx,
              ms,
              new PolarisBaseEntity.Builder(entityWithPath.getEntity()).build(),
              false,
              entityWithPath.getEntity());
      originalEntities.add(entityWithPath.getEntity());
      updatedEntities.add(updatedEntity);
    }

    try {
      ms.writeEntities(callCtx, updatedEntities, originalEntities);
    } catch (RetryOnConcurrencyException e) {
      return new EntitiesResult(
          BaseResult.ReturnStatus.TARGET_ENTITY_CONCURRENTLY_MODIFIED, e.getMessage());
    }

    // good, all success
    return new EntitiesResult(Page.fromItems(updatedEntities));
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
    BasePersistence ms = callCtx.getMetaStore();

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

    // find the entity to rename
    PolarisBaseEntity refreshEntityToRename =
        ms.lookupEntity(
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

    // ensure that nothing exists where we create that entity
    // if this entity already exists, this is an error
    long catalogId =
        newCatalogPath == null || newCatalogPath.size() == 0 ? 0l : newCatalogPath.get(0).getId();
    long parentId =
        newCatalogPath == null || newCatalogPath.size() == 0
            ? 0l
            : newCatalogPath.get(newCatalogPath.size() - 1).getId();
    EntityNameLookupRecord entityActiveRecord =
        ms.lookupEntityIdAndSubTypeByName(
            callCtx,
            catalogId,
            parentId,
            refreshEntityToRename.getTypeCode(),
            renamedEntity.getName());
    if (entityActiveRecord != null) {
      return new EntityResult(
          BaseResult.ReturnStatus.ENTITY_ALREADY_EXISTS, entityActiveRecord.getSubTypeCode());
    }

    // Create a copy of the original before we change its fields so that we can pass in the
    // old version for the persistence layer to work out whether to unlink previous name-lookups
    PolarisBaseEntity.Builder refreshEntityToRenameBuilder =
        new PolarisBaseEntity.Builder(refreshEntityToRename);

    // change its name now
    refreshEntityToRenameBuilder.name(renamedEntity.getName());
    refreshEntityToRenameBuilder.properties(renamedEntity.getProperties());
    refreshEntityToRenameBuilder.internalProperties(renamedEntity.getInternalProperties());

    // re-parent if a new catalog path was specified
    if (newCatalogPath != null) {
      refreshEntityToRenameBuilder.parentId(parentId);
    }

    // persist the entity after change. This will update the lastUpdateTimestamp and bump up the
    // version. Indicate that the nameOrParent changed, so that we also update any by-name
    // lookups if applicable
    PolarisBaseEntity renamedEntityToReturn =
        this.persistEntityAfterChange(
            callCtx, ms, refreshEntityToRenameBuilder.build(), true, refreshEntityToRename);

    // TODO: Use post-validation of source and destination parent paths and/or update the
    // BasePersistence interface to support conditions on entities *other* than the main
    // entity/entities being changed ina a single atomic operation
    return new EntityResult(renamedEntityToReturn);
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
    BasePersistence ms = callCtx.getMetaStore();

    // entity cannot be null
    getDiagnostics().checkNotNull(entityToDrop, "unexpected_null_entity");

    // TODO: Either document allowance of dropping entity concurrently with potentially-impacting
    // changes in the parent path (e.g. race-condition revocation of grants on parent) or
    // add a way for BasePersistence methods to validate conditions on catalogPath entities
    // atomically with the drop.

    // first find the entity to drop
    PolarisBaseEntity refreshEntityToDrop =
        ms.lookupEntity(
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
        // TODO: Come up with atomic solution to blocking dropping of container entities that
        // have children; one option is reference-counting if all child creation/drop operations
        // become two-entity bulk conditional updates that also update the refcount on the parent
        // if not changed concurrently (else retry). In the meantime, there's a window of time
        // where dropping a namespace or container is effectively "recursive" in deleting its
        // children as well if those child entities were created within the short window of
        // the race condition.
        if (ms.hasChildren(callCtx, PolarisEntityType.NAMESPACE, catalogId, catalogId)) {
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
          ms.loadEntities(
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
      if (ms.hasChildren(
          callCtx, null, refreshEntityToDrop.getCatalogId(), refreshEntityToDrop.getId())) {
        return new DropEntityResult(BaseResult.ReturnStatus.NAMESPACE_NOT_EMPTY, null);
      }
    } else if (refreshEntityToDrop.getType() == PolarisEntityType.POLICY && !cleanup) {
      try {
        //  need to check if the policy is attached to any entity
        List<PolarisPolicyMappingRecord> records =
            ms.loadAllTargetsOnPolicy(
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
              .propertiesAsMap(properties)
              .id(ms.generateNewId(callCtx))
              .catalogId(0L)
              .name("entityCleanup_" + entityToDrop.getId())
              .typeCode(PolarisEntityType.TASK.getCode())
              .subTypeCode(PolarisEntitySubType.NULL_SUBTYPE.getCode())
              .createTimestamp(clock.millis());
      if (cleanupProperties != null) {
        taskEntityBuilder.internalPropertiesAsMap(cleanupProperties);
      }
      // TODO: Add a way to create the task entities atomically with dropping the entity;
      // in the meantime, if the server fails partway through a dropEntity, it's possible that
      // the entity is dropped but we don't have any persisted task records that will carry
      // out the cleanup.
      PolarisBaseEntity taskEntity = taskEntityBuilder.build();
      createEntityIfNotExists(callCtx, null, taskEntity);
      return new DropEntityResult(taskEntity.getId());
    }

    // done, return success
    return new DropEntityResult();
  }

  /** {@inheritDoc} */
  @Override
  public @Nonnull PrivilegeResult grantUsageOnRoleToGrantee(
      @Nonnull PolarisCallContext callCtx,
      @Nullable PolarisEntityCore catalog,
      @Nonnull PolarisEntityCore role,
      @Nonnull PolarisEntityCore grantee) {
    // get metastore we should be using
    BasePersistence ms = callCtx.getMetaStore();

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
  public @Nonnull PrivilegeResult revokeUsageOnRoleFromGrantee(
      @Nonnull PolarisCallContext callCtx,
      @Nullable PolarisEntityCore catalog,
      @Nonnull PolarisEntityCore role,
      @Nonnull PolarisEntityCore grantee) {
    // get metastore we should be using
    BasePersistence ms = callCtx.getMetaStore();

    // the usage privilege to revoke
    PolarisPrivilege usagePriv =
        (grantee.getType() == PolarisEntityType.PRINCIPAL_ROLE)
            ? PolarisPrivilege.CATALOG_ROLE_USAGE
            : PolarisPrivilege.PRINCIPAL_ROLE_USAGE;

    // first, ensure that this privilege has been granted
    PolarisGrantRecord grantRecord =
        ms.lookupGrantRecord(
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
  public @Nonnull PrivilegeResult grantPrivilegeOnSecurableToRole(
      @Nonnull PolarisCallContext callCtx,
      @Nonnull PolarisEntityCore grantee,
      @Nullable List<PolarisEntityCore> catalogPath,
      @Nonnull PolarisEntityCore securable,
      @Nonnull PolarisPrivilege privilege) {
    // get metastore we should be using
    BasePersistence ms = callCtx.getMetaStore();

    // grant specified privilege on this securable to this role and return the grant
    PolarisGrantRecord grantRecord =
        this.persistNewGrantRecord(callCtx, ms, securable, grantee, privilege);
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
    BasePersistence ms = callCtx.getMetaStore();

    // lookup the grants records to find this grant
    PolarisGrantRecord grantRecord =
        ms.lookupGrantRecord(
            callCtx,
            securable.getCatalogId(),
            securable.getId(),
            grantee.getCatalogId(),
            grantee.getId(),
            privilege.getCode());

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
  public @Nonnull LoadGrantsResult loadGrantsOnSecurable(
      @Nonnull PolarisCallContext callCtx, PolarisEntityCore securable) {
    return loadGrantsOnSecurable(callCtx, securable.getCatalogId(), securable.getId());
  }

  public @Nonnull LoadGrantsResult loadGrantsOnSecurable(
      @Nonnull PolarisCallContext callCtx, long securableCatalogId, long securableId) {
    // get metastore we should be using
    BasePersistence ms = callCtx.getMetaStore();

    // TODO: Consider whether it's necessary to look up both ends of the grant records atomically
    // or if it's actually safe as two independent lookups.

    // lookup grants version for this securable entity
    int grantsVersion =
        ms.lookupEntityGrantRecordsVersion(callCtx, securableCatalogId, securableId);

    // return null if securable does not exists
    if (grantsVersion == 0) {
      return new LoadGrantsResult(BaseResult.ReturnStatus.ENTITY_NOT_FOUND, null);
    }

    // now fetch all grants for this securable
    final List<PolarisGrantRecord> returnGrantRecords =
        ms.loadAllGrantRecordsOnSecurable(callCtx, securableCatalogId, securableId);

    // find all unique grantees
    List<PolarisEntityId> entityIds =
        returnGrantRecords.stream()
            .map(
                grantRecord ->
                    new PolarisEntityId(
                        grantRecord.getGranteeCatalogId(), grantRecord.getGranteeId()))
            .distinct()
            .collect(Collectors.toList());
    List<PolarisBaseEntity> entities = ms.lookupEntities(callCtx, entityIds);

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
    BasePersistence ms = callCtx.getMetaStore();

    // TODO: Consider whether it's necessary to look up both ends of the grant records atomically
    // or if it's actually safe as two independent lookups.

    // lookup grants version for this grantee entity
    int grantsVersion = ms.lookupEntityGrantRecordsVersion(callCtx, granteeCatalogId, granteeId);

    // return null if grantee does not exists
    if (grantsVersion == 0) {
      return new LoadGrantsResult(BaseResult.ReturnStatus.ENTITY_NOT_FOUND, null);
    }

    // now fetch all grants for this grantee
    final List<PolarisGrantRecord> returnGrantRecords =
        ms.loadAllGrantRecordsOnGrantee(callCtx, granteeCatalogId, granteeId);

    // find all unique securables
    List<PolarisEntityId> entityIds =
        returnGrantRecords.stream()
            .map(
                grantRecord ->
                    new PolarisEntityId(
                        grantRecord.getSecurableCatalogId(), grantRecord.getSecurableId()))
            .distinct()
            .collect(Collectors.toList());
    List<PolarisBaseEntity> entities = ms.lookupEntities(callCtx, entityIds);

    // done, return the list of grants and their version
    return new LoadGrantsResult(
        grantsVersion,
        returnGrantRecords,
        entities.stream().filter(Objects::nonNull).collect(Collectors.toList()));
  }

  /** {@inheritDoc} */
  @Override
  public @Nonnull ChangeTrackingResult loadEntitiesChangeTracking(
      @Nonnull PolarisCallContext callCtx, @Nonnull List<PolarisEntityId> entityIds) {
    // get metastore we should be using
    BasePersistence ms = callCtx.getMetaStore();

    List<PolarisChangeTrackingVersions> changeTracking =
        ms.lookupEntityVersions(callCtx, entityIds);
    return new ChangeTrackingResult(changeTracking);
  }

  /** {@inheritDoc} */
  @Override
  public @Nonnull EntityResult loadEntity(
      @Nonnull PolarisCallContext callCtx,
      long entityCatalogId,
      long entityId,
      @Nonnull PolarisEntityType entityType) {
    // get metastore we should be using
    BasePersistence ms = callCtx.getMetaStore();

    // this is an easy one
    PolarisBaseEntity entity =
        ms.lookupEntity(callCtx, entityCatalogId, entityId, entityType.getCode());
    return (entity != null)
        ? new EntityResult(entity)
        : new EntityResult(BaseResult.ReturnStatus.ENTITY_NOT_FOUND, null);
  }

  @Override
  public @Nonnull EntitiesResult loadTasks(
      @Nonnull PolarisCallContext callCtx, String executorId, PageToken pageToken) {
    BasePersistence ms = callCtx.getMetaStore();

    // find all available tasks
    Page<PolarisBaseEntity> availableTasks =
        ms.loadEntities(
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

    final AtomicInteger failedLeaseCount = new AtomicInteger(0);
    List<PolarisBaseEntity> loadedTasks =
        availableTasks.items().stream()
            .map(
                task -> {
                  PolarisBaseEntity.Builder updatedTaskBuilder =
                      new PolarisBaseEntity.Builder(task);
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
                  updatedTaskBuilder.propertiesAsMap(properties);
                  EntityResult result =
                      updateEntityPropertiesIfNotChanged(callCtx, null, updatedTaskBuilder.build());
                  if (result.getReturnStatus() == BaseResult.ReturnStatus.SUCCESS) {
                    return result.getEntity();
                  } else {
                    failedLeaseCount.getAndIncrement();
                    return null;
                  }
                })
            .filter(Objects::nonNull)
            .collect(Collectors.toList());

    // Since the contract of this method is to only return an empty list once no available tasks
    // are found anymore, if we happen to fail to lease any tasks at all due to all of them
    // hitting concurrency errors, we need to just throw a concurrency exception rather than
    // returning an empty list.
    if (loadedTasks.isEmpty() && failedLeaseCount.get() > 0) {
      // TODO: Currently the contract defined by BasePolarisMetaStoreManagerTest expects either
      // a thrown RetryOnConcurrencyException or else a successful EntitiesResult in the face
      // of concurrent loadTasks calls; this is inconsistent with the rest of the
      // PolarisMetaStoreManager interface where everything is supposed to encapsulate errors
      // in the Result type instead of throwing. But for now, we'll throw.
      throw new RetryOnConcurrencyException(
          "Failed to lease any of %s tasks due to concurrent leases", failedLeaseCount.get());
    }
    return EntitiesResult.fromPage(Page.fromItems(loadedTasks));
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
    BasePersistence ms = callCtx.getMetaStore();
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

    // TODO: Consider whether this independent lookup is safe for the model already or whether
    // we need better atomicity semantics between the base entity and the embedded storage
    // integration.

    // get storage integration
    PolarisStorageIntegration<PolarisStorageConfigurationInfo> storageIntegration =
        ((IntegrationPersistence) ms)
            .loadPolarisStorageIntegration(callCtx, reloadedEntity.getEntity());

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

  /** {@inheritDoc} */
  @Override
  public @Nonnull ResolvedEntityResult loadResolvedEntityById(
      @Nonnull PolarisCallContext callCtx,
      long entityCatalogId,
      long entityId,
      PolarisEntityType entityType) {
    // get metastore we should be using
    BasePersistence ms = callCtx.getMetaStore();

    // load that entity
    PolarisBaseEntity entity =
        ms.lookupEntity(callCtx, entityCatalogId, entityId, entityType.getCode());

    // if entity not found, return null
    if (entity == null) {
      return new ResolvedEntityResult(BaseResult.ReturnStatus.ENTITY_NOT_FOUND, null);
    }

    // TODO: Consider whether it's necessary to look up grant records atomically with the entity
    // or if it's actually safe as independent lookups. Document the nuances of independent lookups.

    // load the grant records
    final List<PolarisGrantRecord> grantRecords;
    if (entity.getType().isGrantee()) {
      grantRecords =
          new ArrayList<>(ms.loadAllGrantRecordsOnGrantee(callCtx, entityCatalogId, entityId));
      grantRecords.addAll(ms.loadAllGrantRecordsOnSecurable(callCtx, entityCatalogId, entityId));
    } else {
      grantRecords = ms.loadAllGrantRecordsOnSecurable(callCtx, entityCatalogId, entityId);
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
    BasePersistence ms = callCtx.getMetaStore();

    // load that entity
    PolarisBaseEntity entity =
        ms.lookupEntityByName(callCtx, entityCatalogId, parentId, entityType.getCode(), entityName);

    // null if entity not found
    if (entity == null) {
      return new ResolvedEntityResult(BaseResult.ReturnStatus.ENTITY_NOT_FOUND, null);
    }

    // TODO: Consider whether it's necessary to look up grant records atomically with the entity
    // or if it's actually safe as independent lookups. Document the nuances of independent lookups.

    // load the grant records
    final List<PolarisGrantRecord> grantRecords;
    if (entity.getType().isGrantee()) {
      grantRecords =
          new ArrayList<>(
              ms.loadAllGrantRecordsOnGrantee(callCtx, entityCatalogId, entity.getId()));
      grantRecords.addAll(
          ms.loadAllGrantRecordsOnSecurable(callCtx, entityCatalogId, entity.getId()));
    } else {
      grantRecords = ms.loadAllGrantRecordsOnSecurable(callCtx, entityCatalogId, entity.getId());
    }

    ResolvedEntityResult result =
        new ResolvedEntityResult(entity, entity.getGrantRecordsVersion(), grantRecords);
    if (PolarisEntityConstants.getRootContainerName().equals(entityName)
        && entityType == PolarisEntityType.ROOT
        && !result.isSuccess()) {
      // Backfill rootContainer if needed.
      PolarisBaseEntity rootContainer =
          new PolarisBaseEntity(
              PolarisEntityConstants.getNullId(),
              PolarisEntityConstants.getRootEntityId(),
              PolarisEntityType.ROOT,
              PolarisEntitySubType.NULL_SUBTYPE,
              PolarisEntityConstants.getRootEntityId(),
              PolarisEntityConstants.getRootContainerName());
      EntityResult backfillResult = this.createEntityIfNotExists(callCtx, null, rootContainer);
      if (backfillResult.isSuccess()) {
        PolarisBaseEntity serviceAdminRole =
            ms.lookupEntityByName(
                callCtx,
                0L,
                0L,
                PolarisEntityType.PRINCIPAL_ROLE.getCode(),
                PolarisEntityConstants.getNameOfPrincipalServiceAdminRole());
        if (serviceAdminRole != null) {
          // TODO: If the original backfill attempt crashes after persisting the root entity
          // but before persisting new grant record, there can be a stuck root-container that
          // lacks the grant to the serviceAdmin. However, nowadays this backfill code might
          // already be obsolete since it was just a holdover when bootstrap didn't create
          // the root container, so consider removing this backfill entirely or else fix
          // the backfill of this serviceAdminRole grant.
          this.persistNewGrantRecord(
              callCtx, ms, rootContainer, serviceAdminRole, PolarisPrivilege.SERVICE_MANAGE_ACCESS);
        }
      }

      // Redo the lookup in a separate read transaction.
      result =
          this.loadResolvedEntityByName(callCtx, entityCatalogId, parentId, entityType, entityName);
    }
    return result;
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
    BasePersistence ms = callCtx.getMetaStore();

    // load version information
    PolarisChangeTrackingVersions entityVersions =
        ms.lookupEntityVersions(callCtx, List.of(new PolarisEntityId(entityCatalogId, entityId)))
            .get(0);

    // if null, the entity has been purged
    if (entityVersions == null) {
      return new ResolvedEntityResult(BaseResult.ReturnStatus.ENTITY_NOT_FOUND, null);
    }

    // load the entity if something changed
    final PolarisBaseEntity entity;
    if (entityVersion != entityVersions.getEntityVersion()) {
      entity = ms.lookupEntity(callCtx, entityCatalogId, entityId, entityType.getCode());

      // if not found, return null
      if (entity == null) {
        return new ResolvedEntityResult(BaseResult.ReturnStatus.ENTITY_NOT_FOUND, null);
      }
    } else {
      // entity has not changed, no need to reload it
      entity = null;
    }

    // TODO: Consider whether it's necessary to look up grant records atomically with the entity
    // or if it's actually safe as independent lookups. Document the nuances of independent lookups.

    // load the grant records if required
    final List<PolarisGrantRecord> grantRecords;
    if (entityVersions.getGrantRecordsVersion() != entityGrantRecordsVersion) {
      if (entityType.isGrantee()) {
        grantRecords =
            new ArrayList<>(ms.loadAllGrantRecordsOnGrantee(callCtx, entityCatalogId, entityId));
        grantRecords.addAll(ms.loadAllGrantRecordsOnSecurable(callCtx, entityCatalogId, entityId));
      } else {
        grantRecords = ms.loadAllGrantRecordsOnSecurable(callCtx, entityCatalogId, entityId);
      }
    } else {
      grantRecords = null;
    }

    // return the result
    return new ResolvedEntityResult(entity, entityVersions.getGrantRecordsVersion(), grantRecords);
  }

  /** {@inheritDoc} */
  @Override
  public <T extends PolarisEntity & LocationBasedEntity>
      Optional<Optional<String>> hasOverlappingSiblings(
          @Nonnull PolarisCallContext callContext, T entity) {
    BasePersistence ms = callContext.getMetaStore();
    return ms.hasOverlappingSiblings(callContext, entity);
  }

  @Override
  public @Nonnull PolicyAttachmentResult attachPolicyToEntity(
      @Nonnull PolarisCallContext callCtx,
      @Nonnull List<PolarisEntityCore> targetCatalogPath,
      @Nonnull PolarisEntityCore target,
      @Nonnull List<PolarisEntityCore> policyCatalogPath,
      @Nonnull PolicyEntity policy,
      Map<String, String> parameters) {
    // get metastore we should be using
    BasePersistence ms = callCtx.getMetaStore();

    return this.persistNewPolicyMappingRecord(callCtx, ms, target, policy, parameters);
  }

  @Override
  public @Nonnull PolicyAttachmentResult detachPolicyFromEntity(
      @Nonnull PolarisCallContext callCtx,
      @Nonnull List<PolarisEntityCore> catalogPath,
      @Nonnull PolarisEntityCore target,
      @Nonnull List<PolarisEntityCore> policyCatalogPath,
      @Nonnull PolicyEntity policy) {
    // get metastore we should be using
    BasePersistence ms = callCtx.getMetaStore();

    PolarisPolicyMappingRecord mappingRecord =
        ms.lookupPolicyMappingRecord(
            callCtx,
            target.getCatalogId(),
            target.getId(),
            policy.getPolicyTypeCode(),
            policy.getCatalogId(),
            policy.getId());
    if (mappingRecord == null) {
      return new PolicyAttachmentResult(BaseResult.ReturnStatus.POLICY_MAPPING_NOT_FOUND, null);
    }

    ms.deleteFromPolicyMappingRecords(callCtx, mappingRecord);

    return new PolicyAttachmentResult(mappingRecord);
  }

  @Override
  public @Nonnull LoadPolicyMappingsResult loadPoliciesOnEntity(
      @Nonnull PolarisCallContext callCtx, @Nonnull PolarisEntityCore target) {
    // get metastore we should be using
    BasePersistence ms = callCtx.getMetaStore();

    PolarisBaseEntity entity =
        ms.lookupEntity(callCtx, target.getCatalogId(), target.getId(), target.getTypeCode());
    if (entity == null) {
      // Target entity does not exist
      return new LoadPolicyMappingsResult(BaseResult.ReturnStatus.ENTITY_NOT_FOUND, null);
    }

    final List<PolarisPolicyMappingRecord> policyMappingRecords =
        ms.loadAllPoliciesOnTarget(callCtx, target.getCatalogId(), target.getId());

    List<PolarisBaseEntity> policyEntities =
        loadPoliciesFromMappingRecords(callCtx, ms, policyMappingRecords);
    return new LoadPolicyMappingsResult(policyMappingRecords, policyEntities);
  }

  @Override
  public @Nonnull LoadPolicyMappingsResult loadPoliciesOnEntityByType(
      @Nonnull PolarisCallContext callCtx,
      @Nonnull PolarisEntityCore target,
      @Nonnull PolicyType policyType) {
    // get metastore we should be using
    BasePersistence ms = callCtx.getMetaStore();

    PolarisBaseEntity entity =
        ms.lookupEntity(callCtx, target.getCatalogId(), target.getId(), target.getTypeCode());
    if (entity == null) {
      // Target entity does not exist
      return new LoadPolicyMappingsResult(BaseResult.ReturnStatus.ENTITY_NOT_FOUND, null);
    }

    final List<PolarisPolicyMappingRecord> policyMappingRecords =
        ms.loadPoliciesOnTargetByType(
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
      @Nonnull BasePersistence ms,
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
      ms.writeToPolicyMappingRecords(callCtx, mappingRecord);
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
      @Nonnull BasePersistence ms,
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
    return ms.lookupEntities(callCtx, policyEntityIds);
  }
}
