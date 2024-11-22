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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.entity.AsyncTaskType;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisChangeTrackingVersions;
import org.apache.polaris.core.entity.PolarisEntitiesActiveKey;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PolarisEntityActiveRecord;
import org.apache.polaris.core.entity.PolarisEntityConstants;
import org.apache.polaris.core.entity.PolarisEntityCore;
import org.apache.polaris.core.entity.PolarisEntityId;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.PolarisGrantRecord;
import org.apache.polaris.core.entity.PolarisPrincipalSecrets;
import org.apache.polaris.core.entity.PolarisPrivilege;
import org.apache.polaris.core.entity.PolarisTaskConstants;
import org.apache.polaris.core.persistence.cache.PolarisRemoteCache.CachedEntryResult;
import org.apache.polaris.core.storage.PolarisCredentialProperty;
import org.apache.polaris.core.storage.PolarisStorageActions;
import org.apache.polaris.core.storage.PolarisStorageConfigurationInfo;
import org.apache.polaris.core.storage.PolarisStorageIntegration;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default implementation of the Polaris Meta Store Manager. Uses the underlying meta store to store
 * and retrieve all Polaris metadata
 */
@SuppressFBWarnings("NP_NULL_ON_SOME_PATH_FROM_RETURN_VALUE")
public class PolarisMetaStoreManagerImpl implements PolarisMetaStoreManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(PolarisMetaStoreManagerImpl.class);

  /** mapper, allows to serialize/deserialize properties to/from JSON */
  private static final ObjectMapper MAPPER = new ObjectMapper();

  /** use synchronous drop for entities */
  private static final boolean USE_SYNCHRONOUS_DROP = true;

  /**
   * Lookup an entity by its name
   *
   * @param callCtx call context
   * @param ms meta store
   * @param entityActiveKey lookup key
   * @return the entity if it exists, null otherwise
   */
  private @Nullable PolarisBaseEntity lookupEntityByName(
      @NotNull PolarisCallContext callCtx,
      @NotNull PolarisMetaStoreSession ms,
      @NotNull PolarisEntitiesActiveKey entityActiveKey) {
    // ensure that the entity exists
    PolarisEntityActiveRecord entityActiveRecord = ms.lookupEntityActive(callCtx, entityActiveKey);

    // if not found, return null
    if (entityActiveRecord == null) {
      return null;
    }

    // lookup the entity, should be there
    PolarisBaseEntity entity =
        ms.lookupEntity(callCtx, entityActiveRecord.getCatalogId(), entityActiveRecord.getId());
    callCtx
        .getDiagServices()
        .checkNotNull(
            entity, "unexpected_not_found_entity", "entityActiveRecord={}", entityActiveRecord);

    // return it now
    return entity;
  }

  /**
   * Write this entity to the meta store.
   *
   * @param callCtx call context
   * @param ms meta store in read/write mode
   * @param entity entity to persist
   * @param writeToActive if true, write it to active
   */
  private void writeEntity(
      @NotNull PolarisCallContext callCtx,
      @NotNull PolarisMetaStoreSession ms,
      @NotNull PolarisBaseEntity entity,
      boolean writeToActive) {
    ms.writeToEntities(callCtx, entity);
    ms.writeToEntitiesChangeTracking(callCtx, entity);

    if (writeToActive) {
      ms.writeToEntitiesActive(callCtx, entity);
    }
  }

  /**
   * Persist the specified new entity. Persist will write this entity in the ENTITIES, in the
   * ENTITIES_ACTIVE and finally in the ENTITIES_CHANGE_TRACKING tables
   *
   * @param callCtx call context
   * @param ms meta store in read/write mode
   * @param entity entity we need a DPO for
   */
  private void persistNewEntity(
      @NotNull PolarisCallContext callCtx,
      @NotNull PolarisMetaStoreSession ms,
      @NotNull PolarisBaseEntity entity) {

    // validate the entity type and subtype
    callCtx.getDiagServices().checkNotNull(entity, "unexpected_null_entity");
    callCtx
        .getDiagServices()
        .checkNotNull(entity.getName(), "unexpected_null_name", "entity={}", entity);
    PolarisEntityType type = PolarisEntityType.fromCode(entity.getTypeCode());
    callCtx.getDiagServices().checkNotNull(type, "unknown_type", "entity={}", entity);
    PolarisEntitySubType subType = PolarisEntitySubType.fromCode(entity.getSubTypeCode());
    callCtx.getDiagServices().checkNotNull(subType, "unexpected_null_subType", "entity={}", entity);
    callCtx
        .getDiagServices()
        .check(
            subType.getParentType() == null || subType.getParentType() == type,
            "invalid_subtype",
            "type={} subType={}",
            type,
            subType);

    // if top-level entity, its parent should be the account
    callCtx
        .getDiagServices()
        .check(
            !type.isTopLevel() || entity.getParentId() == PolarisEntityConstants.getRootEntityId(),
            "top_level_parent_should_be_account",
            "entity={}",
            entity);

    // id should not be null
    callCtx
        .getDiagServices()
        .check(
            entity.getId() != 0 || type == PolarisEntityType.ROOT,
            "id_not_set",
            "entity={}",
            entity);

    // creation timestamp must be filled
    callCtx.getDiagServices().check(entity.getCreateTimestamp() != 0, "null_create_timestamp");

    // this is the first change
    entity.setLastUpdateTimestamp(entity.getCreateTimestamp());

    // set all other timestamps to 0
    entity.setDropTimestamp(0);
    entity.setPurgeTimestamp(0);
    entity.setToPurgeTimestamp(0);

    // write it
    this.writeEntity(callCtx, ms, entity, true);
  }

  /**
   * Persist the specified entity after it has been changed. We will update the last changed time,
   * increment the entity version and persist it back to the ENTITIES and ENTITIES_CHANGE_TRACKING
   * tables
   *
   * @param callCtx call context
   * @param ms meta store
   * @param entity the entity which has been changed
   * @return the entity with its version and lastUpdateTimestamp updated
   */
  private @NotNull PolarisBaseEntity persistEntityAfterChange(
      @NotNull PolarisCallContext callCtx,
      @NotNull PolarisMetaStoreSession ms,
      @NotNull PolarisBaseEntity entity) {

    // validate the entity type and subtype
    callCtx.getDiagServices().checkNotNull(entity, "unexpected_null_entity");
    callCtx
        .getDiagServices()
        .checkNotNull(entity.getName(), "unexpected_null_name", "entity={}", entity);
    PolarisEntityType type = entity.getType();
    callCtx.getDiagServices().checkNotNull(type, "unexpected_null_type", "entity={}", entity);
    PolarisEntitySubType subType = entity.getSubType();
    callCtx.getDiagServices().checkNotNull(subType, "unexpected_null_subType", "entity={}", entity);
    callCtx
        .getDiagServices()
        .check(
            subType.getParentType() == null || subType.getParentType() == type,
            "invalid_subtype",
            "type={} subType={} entity={}",
            type,
            subType,
            entity);

    // entity should not have been dropped
    callCtx
        .getDiagServices()
        .check(entity.getDropTimestamp() == 0, "entity_dropped", "entity={}", entity);

    // creation timestamp must be filled
    long createTimestamp = entity.getCreateTimestamp();
    callCtx
        .getDiagServices()
        .check(createTimestamp != 0, "null_create_timestamp", "entity={}", entity);

    // ensure time is not moving backward...
    long now = System.currentTimeMillis();
    if (now < entity.getCreateTimestamp()) {
      now = entity.getCreateTimestamp() + 1;
    }

    // update last update timestamp and increment entity version
    entity.setLastUpdateTimestamp(now);
    entity.setEntityVersion(entity.getEntityVersion() + 1);

    // persist it to the various slices
    this.writeEntity(callCtx, ms, entity, false);

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
      @NotNull PolarisCallContext callCtx,
      @NotNull PolarisMetaStoreSession ms,
      @NotNull PolarisBaseEntity entity) {

    // validate the entity type and subtype
    callCtx.getDiagServices().checkNotNull(entity, "unexpected_null_dpo");
    callCtx.getDiagServices().checkNotNull(entity.getName(), "unexpected_null_name");

    // creation timestamp must be filled
    callCtx.getDiagServices().check(entity.getDropTimestamp() == 0, "already_dropped");

    // delete it from active slice
    ms.deleteFromEntitiesActive(callCtx, entity);

    // for now drop all entities synchronously
    if (USE_SYNCHRONOUS_DROP) {
      // use synchronous drop

      // delete ALL grant records to (if the entity is a grantee) and from that entity
      final List<PolarisGrantRecord> grantsOnGrantee =
          (entity.getType().isGrantee())
              ? ms.loadAllGrantRecordsOnGrantee(callCtx, entity.getCatalogId(), entity.getId())
              : List.of();
      final List<PolarisGrantRecord> grantsOnSecurable =
          ms.loadAllGrantRecordsOnSecurable(callCtx, entity.getCatalogId(), entity.getId());
      ms.deleteAllEntityGrantRecords(callCtx, entity, grantsOnGrantee, grantsOnSecurable);

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
      for (PolarisBaseEntity entityGrantChanged : entities) {
        entityGrantChanged.setGrantRecordsVersion(entityGrantChanged.getGrantRecordsVersion() + 1);
        ms.writeToEntities(callCtx, entityGrantChanged);
        ms.writeToEntitiesChangeTracking(callCtx, entityGrantChanged);
      }

      // remove the entity being dropped now
      ms.deleteFromEntities(callCtx, entity);
      ms.deleteFromEntitiesChangeTracking(callCtx, entity);

      // if it is a principal, we also need to drop the secrets
      if (entity.getType() == PolarisEntityType.PRINCIPAL) {
        // get internal properties
        Map<String, String> properties =
            this.deserializeProperties(callCtx, entity.getInternalProperties());

        // get client_id
        String clientId = properties.get(PolarisEntityConstants.getClientIdPropertyName());

        // delete it from the secret slice
        ms.deletePrincipalSecrets(callCtx, clientId, entity.getId());
      }
    } else {

      // update the entity to indicate it has been dropped
      final long now = System.currentTimeMillis();
      entity.setDropTimestamp(now);
      entity.setLastUpdateTimestamp(now);

      // schedule purge
      entity.setToPurgeTimestamp(now + PolarisEntityConstants.getRetentionTimeInMs());

      // increment version
      entity.setEntityVersion(entity.getEntityVersion() + 1);

      // write to the dropped slice and to purge slice
      ms.writeToEntities(callCtx, entity);
      ms.writeToEntitiesDropped(callCtx, entity);
      ms.writeToEntitiesChangeTracking(callCtx, entity);
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
  private @NotNull PolarisGrantRecord persistNewGrantRecord(
      @NotNull PolarisCallContext callCtx,
      @NotNull PolarisMetaStoreSession ms,
      @NotNull PolarisEntityCore securable,
      @NotNull PolarisEntityCore grantee,
      @NotNull PolarisPrivilege priv) {

    // validate non null arguments
    callCtx.getDiagServices().checkNotNull(securable, "unexpected_null_securable");
    callCtx.getDiagServices().checkNotNull(grantee, "unexpected_null_grantee");
    callCtx.getDiagServices().checkNotNull(priv, "unexpected_null_priv");

    // ensure that this entity is indeed a grantee like entity
    callCtx
        .getDiagServices()
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
        ms.lookupEntity(callCtx, grantee.getCatalogId(), grantee.getId());
    callCtx
        .getDiagServices()
        .checkNotNull(granteeEntity, "grantee_not_found", "grantee={}", grantee);

    // grants have changed, we need to bump-up the grants version
    granteeEntity.setGrantRecordsVersion(granteeEntity.getGrantRecordsVersion() + 1);
    this.writeEntity(callCtx, ms, granteeEntity, false);

    // we also need to invalidate the grants on that securable so that we can reload them.
    // load the securable and increment its grants version
    PolarisBaseEntity securableEntity =
        ms.lookupEntity(callCtx, securable.getCatalogId(), securable.getId());
    callCtx
        .getDiagServices()
        .checkNotNull(securableEntity, "securable_not_found", "securable={}", securable);

    // grants have changed, we need to bump-up the grants version
    securableEntity.setGrantRecordsVersion(securableEntity.getGrantRecordsVersion() + 1);
    this.writeEntity(callCtx, ms, securableEntity, false);

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
      @NotNull PolarisCallContext callCtx,
      @NotNull PolarisMetaStoreSession ms,
      @NotNull PolarisEntityCore securable,
      @NotNull PolarisEntityCore grantee,
      @NotNull PolarisGrantRecord grantRecord) {

    // validate securable
    callCtx
        .getDiagServices()
        .check(
            securable.getCatalogId() == grantRecord.getSecurableCatalogId()
                && securable.getId() == grantRecord.getSecurableId(),
            "securable_mismatch",
            "securable={} grantRec={}",
            securable,
            grantRecord);

    // validate grantee
    callCtx
        .getDiagServices()
        .check(
            grantee.getCatalogId() == grantRecord.getGranteeCatalogId()
                && grantee.getId() == grantRecord.getGranteeId(),
            "grantee_mismatch",
            "grantee={} grantRec={}",
            grantee,
            grantRecord);

    // ensure the grantee is really a grantee
    callCtx
        .getDiagServices()
        .check(grantee.getType().isGrantee(), "not_a_grantee", "grantee={}", grantee);

    // remove that grant
    ms.deleteFromGrantRecords(callCtx, grantRecord);

    // load the grantee and increment its grants version
    PolarisBaseEntity refreshGrantee =
        ms.lookupEntity(callCtx, grantee.getCatalogId(), grantee.getId());
    callCtx
        .getDiagServices()
        .checkNotNull(
            refreshGrantee, "missing_grantee", "grantRecord={} grantee={}", grantRecord, grantee);

    // grants have changed, we need to bump-up the grants version
    refreshGrantee.setGrantRecordsVersion(refreshGrantee.getGrantRecordsVersion() + 1);
    this.writeEntity(callCtx, ms, refreshGrantee, false);

    // we also need to invalidate the grants on that securable so that we can reload them.
    // load the securable and increment its grants version
    PolarisBaseEntity refreshSecurable =
        ms.lookupEntity(callCtx, securable.getCatalogId(), securable.getId());
    callCtx
        .getDiagServices()
        .checkNotNull(
            refreshSecurable,
            "missing_securable",
            "grantRecord={} securable={}",
            grantRecord,
            securable);

    // grants have changed, we need to bump-up the grants version
    refreshSecurable.setGrantRecordsVersion(refreshSecurable.getGrantRecordsVersion() + 1);
    this.writeEntity(callCtx, ms, refreshSecurable, false);
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
  private @NotNull CreateCatalogResult createCatalog(
      @NotNull PolarisCallContext callCtx,
      @NotNull PolarisMetaStoreSession ms,
      @NotNull PolarisBaseEntity catalog,
      @Nullable PolarisStorageIntegration<?> integration,
      @NotNull List<PolarisEntityCore> principalRoles) {
    // validate input
    callCtx.getDiagServices().checkNotNull(catalog, "unexpected_null_catalog");

    // check if that catalog has already been created
    PolarisBaseEntity refreshCatalog =
        ms.lookupEntity(callCtx, catalog.getCatalogId(), catalog.getId());

    // if found, probably a retry, simply return the previously created catalog
    if (refreshCatalog != null) {
      // if found, ensure it is indeed a catalog
      callCtx
          .getDiagServices()
          .check(
              refreshCatalog.getTypeCode() == PolarisEntityType.CATALOG.getCode(),
              "not_a_catalog",
              "catalog={}",
              catalog);

      // lookup catalog admin role, should exist
      PolarisEntitiesActiveKey adminRoleKey =
          new PolarisEntitiesActiveKey(
              refreshCatalog.getId(),
              refreshCatalog.getId(),
              PolarisEntityType.CATALOG_ROLE.getCode(),
              PolarisEntityConstants.getNameOfCatalogAdminRole());
      PolarisBaseEntity catalogAdminRole = this.lookupEntityByName(callCtx, ms, adminRoleKey);

      // if found, ensure not null
      callCtx
          .getDiagServices()
          .checkNotNull(
              catalogAdminRole, "catalog_admin_role_not_found", "catalog={}", refreshCatalog);

      // done, return the existing catalog
      return new CreateCatalogResult(refreshCatalog, catalogAdminRole);
    }

    // check that a catalog with the same name does not exist already
    PolarisEntitiesActiveKey catalogNameKey =
        new PolarisEntitiesActiveKey(
            PolarisEntityConstants.getNullId(),
            PolarisEntityConstants.getRootEntityId(),
            PolarisEntityType.CATALOG.getCode(),
            catalog.getName());
    PolarisEntityActiveRecord otherCatalogRecord = ms.lookupEntityActive(callCtx, catalogNameKey);

    // if it exists, this is an error, the client should retry
    if (otherCatalogRecord != null) {
      return new CreateCatalogResult(BaseResult.ReturnStatus.ENTITY_ALREADY_EXISTS, null);
    }

    ms.persistStorageIntegrationIfNeeded(callCtx, catalog, integration);

    // now create and persist new catalog entity
    this.persistNewEntity(callCtx, ms, catalog);

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
      PolarisEntitiesActiveKey serviceAdminRoleKey =
          new PolarisEntitiesActiveKey(
              PolarisEntityConstants.getNullId(),
              PolarisEntityConstants.getRootEntityId(),
              PolarisEntityType.PRINCIPAL_ROLE.getCode(),
              PolarisEntityConstants.getNameOfPrincipalServiceAdminRole());
      PolarisBaseEntity serviceAdminRole =
          this.lookupEntityByName(callCtx, ms, serviceAdminRoleKey);
      callCtx.getDiagServices().checkNotNull(serviceAdminRole, "missing_service_admin_role");
      this.persistNewGrantRecord(
          callCtx, ms, adminRole, serviceAdminRole, PolarisPrivilege.CATALOG_ROLE_USAGE);
    } else {
      // grant to each principal role usage on its catalog_admin role
      for (PolarisEntityCore principalRole : principalRoles) {
        // validate not null and really a principal role
        callCtx.getDiagServices().checkNotNull(principalRole, "null principal role");
        callCtx
            .getDiagServices()
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
      @NotNull PolarisCallContext callCtx, @NotNull PolarisMetaStoreSession ms) {

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
    PolarisBaseEntity rootPrincipal =
        new PolarisBaseEntity(
            PolarisEntityConstants.getNullId(),
            rootPrincipalId,
            PolarisEntityType.PRINCIPAL,
            PolarisEntitySubType.NULL_SUBTYPE,
            PolarisEntityConstants.getRootEntityId(),
            PolarisEntityConstants.getRootPrincipalName());

    // create this principal
    this.createPrincipal(callCtx, ms, rootPrincipal);

    // now create the account admin principal role
    long serviceAdminPrincipalRoleId = ms.generateNewId(callCtx);
    PolarisBaseEntity serviceAdminPrincipalRole =
        new PolarisBaseEntity(
            PolarisEntityConstants.getNullId(),
            serviceAdminPrincipalRoleId,
            PolarisEntityType.PRINCIPAL_ROLE,
            PolarisEntitySubType.NULL_SUBTYPE,
            PolarisEntityConstants.getRootEntityId(),
            PolarisEntityConstants.getNameOfPrincipalServiceAdminRole());
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
  public @NotNull BaseResult bootstrapPolarisService(@NotNull PolarisCallContext callCtx) {
    // get meta store we should be using
    PolarisMetaStoreSession ms = callCtx.getMetaStore();

    // run operation in a read/write transaction
    ms.runActionInTransaction(callCtx, () -> this.bootstrapPolarisService(callCtx, ms));

    // all good
    return new BaseResult(BaseResult.ReturnStatus.SUCCESS);
  }

  @Override
  public @NotNull BaseResult purge(@NotNull PolarisCallContext callCtx) {
    // get meta store we should be using
    PolarisMetaStoreSession ms = callCtx.getMetaStore();

    // run operation in a read/write transaction
    LOGGER.warn("Deleting all metadata in the metastore...");
    ms.runActionInTransaction(callCtx, () -> ms.deleteAll(callCtx));
    LOGGER.warn("Finished deleting all metadata in the metastore");

    // all good
    return new BaseResult(BaseResult.ReturnStatus.SUCCESS);
  }

  /**
   * See {@link #readEntityByName(PolarisCallContext, List, PolarisEntityType, PolarisEntitySubType,
   * String)}
   */
  private @NotNull PolarisMetaStoreManager.EntityResult readEntityByName(
      @NotNull PolarisCallContext callCtx,
      @NotNull PolarisMetaStoreSession ms,
      @Nullable List<PolarisEntityCore> catalogPath,
      @NotNull PolarisEntityType entityType,
      @NotNull PolarisEntitySubType entitySubType,
      @NotNull String name) {
    // first resolve again the catalogPath to that entity
    PolarisEntityResolver resolver = new PolarisEntityResolver(callCtx, ms, catalogPath);

    // return if we failed to resolve
    if (resolver.isFailure()) {
      return new EntityResult(BaseResult.ReturnStatus.CATALOG_PATH_CANNOT_BE_RESOLVED, null);
    }

    // now looking the entity by name
    PolarisEntitiesActiveKey entityActiveKey =
        new PolarisEntitiesActiveKey(
            resolver.getCatalogIdOrNull(), resolver.getParentId(), entityType.getCode(), name);
    PolarisBaseEntity entity = this.lookupEntityByName(callCtx, ms, entityActiveKey);

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
  public @NotNull PolarisMetaStoreManager.EntityResult readEntityByName(
      @NotNull PolarisCallContext callCtx,
      @Nullable List<PolarisEntityCore> catalogPath,
      @NotNull PolarisEntityType entityType,
      @NotNull PolarisEntitySubType entitySubType,
      @NotNull String name) {
    // get meta store we should be using
    PolarisMetaStoreSession ms = callCtx.getMetaStore();

    // run operation in a read/write transaction
    return ms.runInReadTransaction(
        callCtx, () -> readEntityByName(callCtx, ms, catalogPath, entityType, entitySubType, name));
  }

  /**
   * See {@link #listEntities(PolarisCallContext, List, PolarisEntityType, PolarisEntitySubType)}
   */
  private @NotNull ListEntitiesResult listEntities(
      @NotNull PolarisCallContext callCtx,
      @NotNull PolarisMetaStoreSession ms,
      @Nullable List<PolarisEntityCore> catalogPath,
      @NotNull PolarisEntityType entityType,
      @NotNull PolarisEntitySubType entitySubType) {
    // first resolve again the catalogPath to that entity
    PolarisEntityResolver resolver = new PolarisEntityResolver(callCtx, ms, catalogPath);

    // return if we failed to resolve
    if (resolver.isFailure()) {
      return new ListEntitiesResult(BaseResult.ReturnStatus.CATALOG_PATH_CANNOT_BE_RESOLVED, null);
    }

    // return list of active entities
    List<PolarisEntityActiveRecord> toreturnList =
        ms.listActiveEntities(
            callCtx, resolver.getCatalogIdOrNull(), resolver.getParentId(), entityType);

    // prune the returned list with only entities matching the entity subtype
    if (entitySubType != PolarisEntitySubType.ANY_SUBTYPE) {
      toreturnList =
          toreturnList.stream()
              .filter(rec -> rec.getSubTypeCode() == entitySubType.getCode())
              .collect(Collectors.toList());
    }

    // done
    return new ListEntitiesResult(toreturnList);
  }

  /** {@inheritDoc} */
  @Override
  public @NotNull ListEntitiesResult listEntities(
      @NotNull PolarisCallContext callCtx,
      @Nullable List<PolarisEntityCore> catalogPath,
      @NotNull PolarisEntityType entityType,
      @NotNull PolarisEntitySubType entitySubType) {
    // get meta store we should be using
    PolarisMetaStoreSession ms = callCtx.getMetaStore();

    // run operation in a read transaction
    return ms.runInReadTransaction(
        callCtx, () -> listEntities(callCtx, ms, catalogPath, entityType, entitySubType));
  }

  /** {@inheritDoc} */
  @Override
  public @NotNull GenerateEntityIdResult generateNewEntityId(@NotNull PolarisCallContext callCtx) {
    // get meta store we should be using
    PolarisMetaStoreSession ms = callCtx.getMetaStore();

    return new GenerateEntityIdResult(ms.generateNewId(callCtx));
  }

  /**
   * Given the internal property as a map of key/value pairs, serialize it to a String
   *
   * @param properties a map of key/value pairs
   * @return a String, the JSON representation of the map
   */
  public String serializeProperties(PolarisCallContext callCtx, Map<String, String> properties) {

    String jsonString = null;
    try {
      // Deserialize the JSON string to a Map<String, String>
      jsonString = MAPPER.writeValueAsString(properties);
    } catch (JsonProcessingException ex) {
      callCtx.getDiagServices().fail("got_json_processing_exception", "ex={}", ex);
    }

    return jsonString;
  }

  /**
   * Given the serialized properties, deserialize those to a {@code Map<String, String>}
   *
   * @param properties a JSON string representing the set of properties
   * @return a Map of string
   */
  public Map<String, String> deserializeProperties(PolarisCallContext callCtx, String properties) {

    Map<String, String> retProperties = null;
    try {
      // Deserialize the JSON string to a Map<String, String>
      retProperties = MAPPER.readValue(properties, new TypeReference<>() {});
    } catch (JsonMappingException ex) {
      callCtx.getDiagServices().fail("got_json_mapping_exception", "ex={}", ex);
    } catch (JsonProcessingException ex) {
      callCtx.getDiagServices().fail("got_json_processing_exception", "ex={}", ex);
    }

    return retProperties;
  }

  /** {@link #createPrincipal(PolarisCallContext, PolarisBaseEntity)} */
  private @NotNull CreatePrincipalResult createPrincipal(
      @NotNull PolarisCallContext callCtx,
      @NotNull PolarisMetaStoreSession ms,
      @NotNull PolarisBaseEntity principal) {
    // validate input
    callCtx.getDiagServices().checkNotNull(principal, "unexpected_null_principal");

    // check if that catalog has already been created
    PolarisBaseEntity refreshPrincipal =
        ms.lookupEntity(callCtx, principal.getCatalogId(), principal.getId());

    // if found, probably a retry, simply return the previously created principal
    if (refreshPrincipal != null) {
      // if found, ensure it is indeed a principal
      callCtx
          .getDiagServices()
          .check(
              principal.getTypeCode() == PolarisEntityType.PRINCIPAL.getCode(),
              "not_a_principal",
              "principal={}",
              principal);

      // get internal properties
      Map<String, String> properties =
          this.deserializeProperties(callCtx, refreshPrincipal.getInternalProperties());

      // get client_id
      String clientId = properties.get(PolarisEntityConstants.getClientIdPropertyName());

      // should not be null
      callCtx
          .getDiagServices()
          .checkNotNull(
              clientId,
              "null_client_id",
              "properties={}",
              refreshPrincipal.getInternalProperties());
      // ensure non null and non empty
      callCtx
          .getDiagServices()
          .check(
              !clientId.isEmpty(),
              "empty_client_id",
              "properties={}",
              refreshPrincipal.getInternalProperties());

      // get the main and secondary secrets for that client
      PolarisPrincipalSecrets principalSecrets = ms.loadPrincipalSecrets(callCtx, clientId);

      // should not be null
      callCtx
          .getDiagServices()
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
    PolarisEntitiesActiveKey principalNameKey =
        new PolarisEntitiesActiveKey(
            PolarisEntityConstants.getNullId(),
            PolarisEntityConstants.getRootEntityId(),
            PolarisEntityType.PRINCIPAL.getCode(),
            principal.getName());
    PolarisEntityActiveRecord otherPrincipalRecord =
        ms.lookupEntityActive(callCtx, principalNameKey);

    // if it exists, this is an error, the client should retry
    if (otherPrincipalRecord != null) {
      return new CreatePrincipalResult(BaseResult.ReturnStatus.ENTITY_ALREADY_EXISTS, null);
    }

    // generate new secrets for this principal
    PolarisPrincipalSecrets principalSecrets =
        ms.generateNewPrincipalSecrets(callCtx, principal.getName(), principal.getId());

    // generate properties
    Map<String, String> internalProperties = getInternalPropertyMap(callCtx, principal);
    internalProperties.put(
        PolarisEntityConstants.getClientIdPropertyName(), principalSecrets.getPrincipalClientId());

    // remember client id
    principal.setInternalProperties(this.serializeProperties(callCtx, internalProperties));

    // now create and persist new catalog entity
    this.persistNewEntity(callCtx, ms, principal);

    // success, return the two entities
    return new CreatePrincipalResult(principal, principalSecrets);
  }

  /** {@inheritDoc} */
  @Override
  public @NotNull CreatePrincipalResult createPrincipal(
      @NotNull PolarisCallContext callCtx, @NotNull PolarisBaseEntity principal) {
    // get metastore we should be using
    PolarisMetaStoreSession ms = callCtx.getMetaStore();

    // need to run inside a read/write transaction
    return ms.runInTransaction(callCtx, () -> this.createPrincipal(callCtx, ms, principal));
  }

  /** See {@link #loadPrincipalSecrets(PolarisCallContext, String)} */
  private @Nullable PolarisPrincipalSecrets loadPrincipalSecrets(
      @NotNull PolarisCallContext callCtx, PolarisMetaStoreSession ms, @NotNull String clientId) {
    return ms.loadPrincipalSecrets(callCtx, clientId);
  }

  /** {@inheritDoc} */
  @Override
  public @NotNull PrincipalSecretsResult loadPrincipalSecrets(
      @NotNull PolarisCallContext callCtx, @NotNull String clientId) {
    // get metastore we should be using
    PolarisMetaStoreSession ms = callCtx.getMetaStore();

    // need to run inside a read/write transaction
    PolarisPrincipalSecrets secrets =
        ms.runInTransaction(callCtx, () -> this.loadPrincipalSecrets(callCtx, ms, clientId));

    return (secrets == null)
        ? new PrincipalSecretsResult(BaseResult.ReturnStatus.ENTITY_NOT_FOUND, null)
        : new PrincipalSecretsResult(secrets);
  }

  /** See {@link #} */
  private @Nullable PolarisPrincipalSecrets rotatePrincipalSecrets(
      @NotNull PolarisCallContext callCtx,
      @NotNull PolarisMetaStoreSession ms,
      @NotNull String clientId,
      long principalId,
      boolean reset,
      @NotNull String oldSecretHash) {
    // if not found, the principal must have been dropped
    EntityResult loadEntityResult =
        loadEntity(callCtx, ms, PolarisEntityConstants.getNullId(), principalId);
    if (loadEntityResult.getReturnStatus() != BaseResult.ReturnStatus.SUCCESS) {
      return null;
    }

    PolarisBaseEntity principal = loadEntityResult.getEntity();
    Map<String, String> internalProps =
        PolarisObjectMapperUtil.deserializeProperties(
            callCtx,
            principal.getInternalProperties() == null ? "{}" : principal.getInternalProperties());

    boolean doReset =
        reset
            || internalProps.get(
                    PolarisEntityConstants.PRINCIPAL_CREDENTIAL_ROTATION_REQUIRED_STATE)
                != null;
    PolarisPrincipalSecrets secrets =
        ms.rotatePrincipalSecrets(callCtx, clientId, principalId, doReset, oldSecretHash);

    if (reset
        && !internalProps.containsKey(
            PolarisEntityConstants.PRINCIPAL_CREDENTIAL_ROTATION_REQUIRED_STATE)) {
      internalProps.put(
          PolarisEntityConstants.PRINCIPAL_CREDENTIAL_ROTATION_REQUIRED_STATE, "true");
      principal.setInternalProperties(
          PolarisObjectMapperUtil.serializeProperties(callCtx, internalProps));
      principal.setEntityVersion(principal.getEntityVersion() + 1);
      writeEntity(callCtx, ms, principal, true);
    } else if (internalProps.containsKey(
        PolarisEntityConstants.PRINCIPAL_CREDENTIAL_ROTATION_REQUIRED_STATE)) {
      internalProps.remove(PolarisEntityConstants.PRINCIPAL_CREDENTIAL_ROTATION_REQUIRED_STATE);
      principal.setInternalProperties(
          PolarisObjectMapperUtil.serializeProperties(callCtx, internalProps));
      principal.setEntityVersion(principal.getEntityVersion() + 1);
      writeEntity(callCtx, ms, principal, true);
    }
    return secrets;
  }

  /** {@inheritDoc} */
  @Override
  public @NotNull PrincipalSecretsResult rotatePrincipalSecrets(
      @NotNull PolarisCallContext callCtx,
      @NotNull String clientId,
      long principalId,
      boolean reset,
      @NotNull String oldSecretHash) {
    // get metastore we should be using
    PolarisMetaStoreSession ms = callCtx.getMetaStore();

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

  /** {@inheritDoc} */
  @Override
  public @NotNull CreateCatalogResult createCatalog(
      @NotNull PolarisCallContext callCtx,
      @NotNull PolarisBaseEntity catalog,
      @NotNull List<PolarisEntityCore> principalRoles) {
    // get metastore we should be using
    PolarisMetaStoreSession ms = callCtx.getMetaStore();

    Map<String, String> internalProp = getInternalPropertyMap(callCtx, catalog);
    String integrationIdentifierOrId =
        internalProp.get(PolarisEntityConstants.getStorageIntegrationIdentifierPropertyName());
    String storageConfigInfoStr =
        internalProp.get(PolarisEntityConstants.getStorageConfigInfoPropertyName());
    PolarisStorageIntegration<?> integration;
    // storageConfigInfo's presence is needed to create a storage integration
    // and the catalog should not have an internal property of storage identifier or id yet
    if (storageConfigInfoStr != null && integrationIdentifierOrId == null) {
      integration =
          ms.createStorageIntegration(
              callCtx,
              catalog.getCatalogId(),
              catalog.getId(),
              PolarisStorageConfigurationInfo.deserialize(
                  callCtx.getDiagServices(), storageConfigInfoStr));
    } else {
      integration = null;
    }
    // need to run inside a read/write transaction
    return ms.runInTransaction(
        callCtx, () -> this.createCatalog(callCtx, ms, catalog, integration, principalRoles));
  }

  /** {@link #createEntityIfNotExists(PolarisCallContext, List, PolarisBaseEntity)} */
  private @NotNull EntityResult createEntityIfNotExists(
      @NotNull PolarisCallContext callCtx,
      @NotNull PolarisMetaStoreSession ms,
      @Nullable List<PolarisEntityCore> catalogPath,
      @NotNull PolarisBaseEntity entity) {

    // entity cannot be null
    callCtx.getDiagServices().checkNotNull(entity, "unexpected_null_entity");

    // entity name must be specified
    callCtx.getDiagServices().checkNotNull(entity.getName(), "unexpected_null_entity_name");

    // first, check if the entity has already been created, in which case we will simply return it
    PolarisBaseEntity entityFound = ms.lookupEntity(callCtx, entity.getCatalogId(), entity.getId());
    if (entityFound != null) {
      // probably the client retried, simply return it
      return new EntityResult(entityFound);
    }

    // first resolve again the catalogPath
    PolarisEntityResolver resolver = new PolarisEntityResolver(callCtx, ms, catalogPath);

    // return if we failed to resolve
    if (resolver.isFailure()) {
      return new EntityResult(BaseResult.ReturnStatus.CATALOG_PATH_CANNOT_BE_RESOLVED, null);
    }

    // check if an entity does not already exist with the same name. If true, this is an error
    PolarisEntitiesActiveKey entityActiveKey =
        new PolarisEntitiesActiveKey(
            entity.getCatalogId(),
            entity.getParentId(),
            entity.getType().getCode(),
            entity.getName());
    PolarisEntityActiveRecord entityActiveRecord = ms.lookupEntityActive(callCtx, entityActiveKey);
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
  public @NotNull EntityResult createEntityIfNotExists(
      @NotNull PolarisCallContext callCtx,
      @Nullable List<PolarisEntityCore> catalogPath,
      @NotNull PolarisBaseEntity entity) {
    // get metastore we should be using
    PolarisMetaStoreSession ms = callCtx.getMetaStore();

    // need to run inside a read/write transaction
    return ms.runInTransaction(
        callCtx, () -> this.createEntityIfNotExists(callCtx, ms, catalogPath, entity));
  }

  @Override
  public @NotNull EntitiesResult createEntitiesIfNotExist(
      @NotNull PolarisCallContext callCtx,
      @Nullable List<PolarisEntityCore> catalogPath,
      @NotNull List<? extends PolarisBaseEntity> entities) {
    // get metastore we should be using
    PolarisMetaStoreSession ms = callCtx.getMetaStore();

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
          return new EntitiesResult(createdEntities);
        });
  }

  /**
   * See {@link #updateEntityPropertiesIfNotChanged(PolarisCallContext, List, PolarisBaseEntity)}
   */
  private @NotNull EntityResult updateEntityPropertiesIfNotChanged(
      @NotNull PolarisCallContext callCtx,
      @NotNull PolarisMetaStoreSession ms,
      @Nullable List<PolarisEntityCore> catalogPath,
      @NotNull PolarisBaseEntity entity) {
    // entity cannot be null
    callCtx.getDiagServices().checkNotNull(entity, "unexpected_null_entity");

    // re-resolve everything including that entity
    PolarisEntityResolver resolver = new PolarisEntityResolver(callCtx, ms, catalogPath, entity);

    // if resolution failed, return false
    if (resolver.isFailure()) {
      return new EntityResult(BaseResult.ReturnStatus.CATALOG_PATH_CANNOT_BE_RESOLVED, null);
    }

    // lookup the entity, cannot be null
    PolarisBaseEntity entityRefreshed =
        ms.lookupEntity(callCtx, entity.getCatalogId(), entity.getId());
    callCtx
        .getDiagServices()
        .checkNotNull(entityRefreshed, "unexpected_entity_not_found", "entity={}", entity);

    // check that the version of the entity has not changed at all to avoid concurrent updates
    if (entityRefreshed.getEntityVersion() != entity.getEntityVersion()) {
      return new EntityResult(BaseResult.ReturnStatus.TARGET_ENTITY_CONCURRENTLY_MODIFIED, null);
    }

    // update the two properties
    entityRefreshed.setInternalProperties(entity.getInternalProperties());
    entityRefreshed.setProperties(entity.getProperties());

    // persist this entity after changing it. This will update the version and update the last
    // updated time. Because the entity version is changed, we will update the change tracking table
    PolarisBaseEntity persistedEntity = this.persistEntityAfterChange(callCtx, ms, entityRefreshed);
    return new EntityResult(persistedEntity);
  }

  /** {@inheritDoc} */
  @Override
  public @NotNull EntityResult updateEntityPropertiesIfNotChanged(
      @NotNull PolarisCallContext callCtx,
      @Nullable List<PolarisEntityCore> catalogPath,
      @NotNull PolarisBaseEntity entity) {
    // get metastore we should be using
    PolarisMetaStoreSession ms = callCtx.getMetaStore();

    // need to run inside a read/write transaction
    return ms.runInTransaction(
        callCtx, () -> this.updateEntityPropertiesIfNotChanged(callCtx, ms, catalogPath, entity));
  }

  /** See {@link #updateEntitiesPropertiesIfNotChanged(PolarisCallContext, List)} */
  private @NotNull EntitiesResult updateEntitiesPropertiesIfNotChanged(
      @NotNull PolarisCallContext callCtx,
      @NotNull PolarisMetaStoreSession ms,
      @NotNull List<EntityWithPath> entities) {
    // ensure that the entities list is not null
    callCtx.getDiagServices().checkNotNull(entities, "unexpected_null_entities");

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
    return new EntitiesResult(updatedEntities);
  }

  /** {@inheritDoc} */
  @Override
  public @NotNull EntitiesResult updateEntitiesPropertiesIfNotChanged(
      @NotNull PolarisCallContext callCtx, @NotNull List<EntityWithPath> entities) {
    // get metastore we should be using
    PolarisMetaStoreSession ms = callCtx.getMetaStore();

    // need to run inside a read/write transaction
    return ms.runInTransaction(
        callCtx, () -> this.updateEntitiesPropertiesIfNotChanged(callCtx, ms, entities));
  }

  /**
   * See {@link PolarisMetaStoreManager#renameEntity(PolarisCallContext, List, PolarisEntityCore,
   * List, PolarisEntity)}
   */
  private @NotNull EntityResult renameEntity(
      @NotNull PolarisCallContext callCtx,
      @NotNull PolarisMetaStoreSession ms,
      @Nullable List<PolarisEntityCore> catalogPath,
      @NotNull PolarisEntityCore entityToRename,
      @Nullable List<PolarisEntityCore> newCatalogPath,
      @NotNull PolarisBaseEntity renamedEntity) {

    // entity and new name cannot be null
    callCtx.getDiagServices().checkNotNull(entityToRename, "unexpected_null_entityToRename");
    callCtx.getDiagServices().checkNotNull(renamedEntity, "unexpected_null_renamedEntity");

    // if a new catalog path is specified (i.e. re-parent operation), a catalog path should be
    // specified too
    callCtx
        .getDiagServices()
        .check(
            (newCatalogPath == null) || (catalogPath != null),
            "newCatalogPath_specified_without_catalogPath");

    // null is shorthand for saying the path isn't changing
    if (newCatalogPath == null) {
      newCatalogPath = catalogPath;
    }

    // re-resolve everything including that entity
    PolarisEntityResolver resolver =
        new PolarisEntityResolver(callCtx, ms, catalogPath, entityToRename);

    // if resolution failed, return false
    if (resolver.isFailure()) {
      return new EntityResult(BaseResult.ReturnStatus.ENTITY_CANNOT_BE_RESOLVED, null);
    }

    // find the entity to rename
    PolarisBaseEntity refreshEntityToRename =
        ms.lookupEntity(callCtx, entityToRename.getCatalogId(), entityToRename.getId());

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
      resolver = new PolarisEntityResolver(callCtx, ms, newCatalogPath);

      // if resolution failed, return false
      if (resolver.isFailure()) {
        return new EntityResult(BaseResult.ReturnStatus.CATALOG_PATH_CANNOT_BE_RESOLVED, null);
      }
    }

    // ensure that nothing exists where we create that entity
    PolarisEntitiesActiveKey entityActiveKey =
        new PolarisEntitiesActiveKey(
            resolver.getCatalogIdOrNull(),
            resolver.getParentId(),
            refreshEntityToRename.getTypeCode(),
            renamedEntity.getName());
    // if this entity already exists, this is an error
    PolarisEntityActiveRecord entityActiveRecord = ms.lookupEntityActive(callCtx, entityActiveKey);
    if (entityActiveRecord != null) {
      return new EntityResult(
          BaseResult.ReturnStatus.ENTITY_ALREADY_EXISTS, entityActiveRecord.getSubTypeCode());
    }

    // all good, delete the existing entity from the active slice
    ms.deleteFromEntitiesActive(callCtx, refreshEntityToRename);

    // change its name now
    refreshEntityToRename.setName(renamedEntity.getName());
    refreshEntityToRename.setProperties(renamedEntity.getProperties());
    refreshEntityToRename.setInternalProperties(renamedEntity.getInternalProperties());

    // re-parent if a new catalog path was specified
    if (newCatalogPath != null) {
      refreshEntityToRename.setParentId(resolver.getParentId());
    }

    // persist back to the active slice with its new name and parent
    ms.writeToEntitiesActive(callCtx, refreshEntityToRename);

    // persist the entity after change. This wil update the lastUpdateTimestamp and bump up the
    // version
    PolarisBaseEntity renamedEntityToReturn =
        this.persistEntityAfterChange(callCtx, ms, refreshEntityToRename);
    return new EntityResult(renamedEntityToReturn);
  }

  /** {@inheritDoc} */
  @Override
  public @NotNull EntityResult renameEntity(
      @NotNull PolarisCallContext callCtx,
      @Nullable List<PolarisEntityCore> catalogPath,
      @NotNull PolarisEntityCore entityToRename,
      @Nullable List<PolarisEntityCore> newCatalogPath,
      @NotNull PolarisEntity renamedEntity) {
    // get metastore we should be using
    PolarisMetaStoreSession ms = callCtx.getMetaStore();

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
   * <p>{@link #dropEntityIfExists(PolarisCallContext, List, PolarisEntityCore, Map, boolean)}
   */
  private @NotNull DropEntityResult dropEntityIfExists(
      @NotNull PolarisCallContext callCtx,
      @NotNull PolarisMetaStoreSession ms,
      @Nullable List<PolarisEntityCore> catalogPath,
      @NotNull PolarisEntityCore entityToDrop,
      @Nullable Map<String, String> cleanupProperties,
      boolean cleanup) {
    // entity cannot be null
    callCtx.getDiagServices().checkNotNull(entityToDrop, "unexpected_null_entity");

    // re-resolve everything including that entity
    PolarisEntityResolver resolver =
        new PolarisEntityResolver(callCtx, ms, catalogPath, entityToDrop);

    // if resolution failed, return false
    if (resolver.isFailure()) {
      return new DropEntityResult(BaseResult.ReturnStatus.CATALOG_PATH_CANNOT_BE_RESOLVED, null);
    }

    // first find the entity to drop
    PolarisBaseEntity refreshEntityToDrop =
        ms.lookupEntity(callCtx, entityToDrop.getCatalogId(), entityToDrop.getId());

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

      // if not all namespaces are dropped, we cannot drop this catalog
      if (ms.hasChildren(callCtx, PolarisEntityType.NAMESPACE, catalogId, catalogId)) {
        return new DropEntityResult(BaseResult.ReturnStatus.NAMESPACE_NOT_EMPTY, null);
      }

      // get the list of catalog roles, at most 2
      List<PolarisBaseEntity> catalogRoles =
          ms.listActiveEntities(
              callCtx,
              catalogId,
              catalogId,
              PolarisEntityType.CATALOG_ROLE,
              2,
              entity -> true,
              Function.identity());

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
    }

    // simply delete that entity. Will be removed from entities_active, added to the
    // entities_dropped and its version will be changed.
    this.dropEntity(callCtx, ms, refreshEntityToDrop);

    // if cleanup, schedule a cleanup task for the entity. do this here, so that drop and scheduling
    // the cleanup task is transactional. Otherwise, we'll be unable to schedule the cleanup task
    // later
    if (cleanup) {
      PolarisBaseEntity taskEntity =
          new PolarisEntity.Builder()
              .setId(generateNewEntityId(callCtx).getId())
              .setCatalogId(0L)
              .setName("entityCleanup_" + entityToDrop.getId())
              .setType(PolarisEntityType.TASK)
              .setSubType(PolarisEntitySubType.NULL_SUBTYPE)
              .setCreateTimestamp(callCtx.getClock().millis())
              .build();

      Map<String, String> properties = new HashMap<>();
      properties.put(
          PolarisTaskConstants.TASK_TYPE,
          String.valueOf(AsyncTaskType.ENTITY_CLEANUP_SCHEDULER.typeCode()));
      properties.put("data", PolarisObjectMapperUtil.serialize(callCtx, refreshEntityToDrop));
      taskEntity.setProperties(PolarisObjectMapperUtil.serializeProperties(callCtx, properties));
      if (cleanupProperties != null) {
        taskEntity.setInternalProperties(
            PolarisObjectMapperUtil.serializeProperties(callCtx, cleanupProperties));
      }
      createEntityIfNotExists(callCtx, ms, null, taskEntity);
      return new DropEntityResult(taskEntity.getId());
    }

    // done, return success
    return new DropEntityResult();
  }

  /** {@inheritDoc} */
  @Override
  public @NotNull DropEntityResult dropEntityIfExists(
      @NotNull PolarisCallContext callCtx,
      @Nullable List<PolarisEntityCore> catalogPath,
      @NotNull PolarisEntityCore entityToDrop,
      @Nullable Map<String, String> cleanupProperties,
      boolean cleanup) {
    // get metastore we should be using
    PolarisMetaStoreSession ms = callCtx.getMetaStore();

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
  private @NotNull PolarisEntityResolver resolveRoleToGranteeUsageGrant(
      @NotNull PolarisCallContext callCtx,
      @NotNull PolarisMetaStoreSession ms,
      @Nullable PolarisEntityCore catalog,
      @NotNull PolarisEntityCore role,
      @NotNull PolarisEntityCore grantee) {

    // validate the grantee input
    callCtx.getDiagServices().checkNotNull(grantee, "unexpected_null_grantee");
    callCtx
        .getDiagServices()
        .check(grantee.getType().isGrantee(), "not_a_grantee", "grantee={}", grantee);

    // validate role
    callCtx.getDiagServices().checkNotNull(role, "unexpected_null_role");

    // role should be a catalog or a principal role
    boolean isCatalogRole = role.getTypeCode() == PolarisEntityType.CATALOG_ROLE.getCode();
    boolean isPrincipalRole = role.getTypeCode() == PolarisEntityType.PRINCIPAL_ROLE.getCode();
    callCtx.getDiagServices().check(isCatalogRole || isPrincipalRole, "not_a_role");

    // if the role is a catalog role, ensure a catalog is specified and
    // vice-versa, catalog should be null if the role is a principal role
    callCtx
        .getDiagServices()
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
        callCtx, ms, catalog != null ? List.of(catalog) : null, null, otherTopLevelEntities);
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
      @NotNull PolarisCallContext callCtx,
      @NotNull PolarisMetaStoreSession ms,
      @NotNull PolarisEntityCore grantee,
      @Nullable List<PolarisEntityCore> catalogPath,
      @NotNull PolarisEntityCore securable) {
    // validate role input
    callCtx.getDiagServices().checkNotNull(grantee, "unexpected_null_grantee");
    callCtx
        .getDiagServices()
        .check(grantee.getType().isGrantee(), "not_grantee_type", "grantee={}", grantee);

    // securable must be supplied
    callCtx.getDiagServices().checkNotNull(securable, "unexpected_null_securable");
    if (securable.getCatalogId() > 0) {
      // catalogPath must be supplied if the securable has a catalogId
      callCtx.getDiagServices().checkNotNull(catalogPath, "unexpected_null_catalogPath");
    }

    // re-resolve now all these entities
    return new PolarisEntityResolver(callCtx, ms, catalogPath, securable, List.of(grantee));
  }

  /**
   * See {@link #grantUsageOnRoleToGrantee(PolarisCallContext, PolarisEntityCore, PolarisEntityCore,
   * PolarisEntityCore)}
   */
  private @NotNull PrivilegeResult grantUsageOnRoleToGrantee(
      @NotNull PolarisCallContext callCtx,
      @NotNull PolarisMetaStoreSession ms,
      @Nullable PolarisEntityCore catalog,
      @NotNull PolarisEntityCore role,
      @NotNull PolarisEntityCore grantee) {

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
    callCtx
        .getDiagServices()
        .check(grantee.getType().isGrantee(), "not_a_grantee", "grantee={}", grantee);
    PolarisGrantRecord grantRecord =
        this.persistNewGrantRecord(callCtx, ms, role, grantee, usagePriv);
    return new PrivilegeResult(grantRecord);
  }

  /** {@inheritDoc} */
  @Override
  public @NotNull PrivilegeResult grantUsageOnRoleToGrantee(
      @NotNull PolarisCallContext callCtx,
      @Nullable PolarisEntityCore catalog,
      @NotNull PolarisEntityCore role,
      @NotNull PolarisEntityCore grantee) {
    // get metastore we should be using
    PolarisMetaStoreSession ms = callCtx.getMetaStore();

    // need to run inside a read/write transaction
    return ms.runInTransaction(
        callCtx, () -> this.grantUsageOnRoleToGrantee(callCtx, ms, catalog, role, grantee));
  }

  /**
   * See {@link #revokeUsageOnRoleFromGrantee(PolarisCallContext, PolarisEntityCore,
   * PolarisEntityCore, PolarisEntityCore)}
   */
  private @NotNull PrivilegeResult revokeUsageOnRoleFromGrantee(
      @NotNull PolarisCallContext callCtx,
      @NotNull PolarisMetaStoreSession ms,
      @Nullable PolarisEntityCore catalog,
      @NotNull PolarisEntityCore role,
      @NotNull PolarisEntityCore grantee) {

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
  public @NotNull PrivilegeResult revokeUsageOnRoleFromGrantee(
      @NotNull PolarisCallContext callCtx,
      @Nullable PolarisEntityCore catalog,
      @NotNull PolarisEntityCore role,
      @NotNull PolarisEntityCore grantee) {
    // get metastore we should be using
    PolarisMetaStoreSession ms = callCtx.getMetaStore();

    // need to run inside a read/write transaction
    return ms.runInTransaction(
        callCtx, () -> this.revokeUsageOnRoleFromGrantee(callCtx, ms, catalog, role, grantee));
  }

  /**
   * See {@link #grantPrivilegeOnSecurableToRole(PolarisCallContext, PolarisEntityCore, List,
   * PolarisEntityCore, PolarisPrivilege)}
   */
  private @NotNull PrivilegeResult grantPrivilegeOnSecurableToRole(
      @NotNull PolarisCallContext callCtx,
      @NotNull PolarisMetaStoreSession ms,
      @NotNull PolarisEntityCore grantee,
      @Nullable List<PolarisEntityCore> catalogPath,
      @NotNull PolarisEntityCore securable,
      @NotNull PolarisPrivilege priv) {

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
  public @NotNull PrivilegeResult grantPrivilegeOnSecurableToRole(
      @NotNull PolarisCallContext callCtx,
      @NotNull PolarisEntityCore grantee,
      @Nullable List<PolarisEntityCore> catalogPath,
      @NotNull PolarisEntityCore securable,
      @NotNull PolarisPrivilege privilege) {
    // get metastore we should be using
    PolarisMetaStoreSession ms = callCtx.getMetaStore();

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
  private @NotNull PrivilegeResult revokePrivilegeOnSecurableFromRole(
      @NotNull PolarisCallContext callCtx,
      @NotNull PolarisMetaStoreSession ms,
      @NotNull PolarisEntityCore grantee,
      @Nullable List<PolarisEntityCore> catalogPath,
      @NotNull PolarisEntityCore securable,
      @NotNull PolarisPrivilege priv) {

    // re-resolve now all these entities
    PolarisEntityResolver resolver =
        this.resolveSecurableToRoleGrant(callCtx, ms, grantee, catalogPath, securable);

    // if failure to resolve, let the caller know
    if (resolver.isFailure()) {
      return new PrivilegeResult(BaseResult.ReturnStatus.ENTITY_CANNOT_BE_RESOLVED, null);
    }

    // lookup the grants records to find this grant
    PolarisGrantRecord grantRecord =
        ms.lookupGrantRecord(
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
  public @NotNull PrivilegeResult revokePrivilegeOnSecurableFromRole(
      @NotNull PolarisCallContext callCtx,
      @NotNull PolarisEntityCore grantee,
      @Nullable List<PolarisEntityCore> catalogPath,
      @NotNull PolarisEntityCore securable,
      @NotNull PolarisPrivilege privilege) {
    // get metastore we should be using
    PolarisMetaStoreSession ms = callCtx.getMetaStore();

    // need to run inside a read/write transaction
    return ms.runInTransaction(
        callCtx,
        () ->
            this.revokePrivilegeOnSecurableFromRole(
                callCtx, ms, grantee, catalogPath, securable, privilege));
  }

  /** {@link #loadGrantsOnSecurable(PolarisCallContext, long, long)} */
  private @NotNull LoadGrantsResult loadGrantsOnSecurable(
      @NotNull PolarisCallContext callCtx,
      @NotNull PolarisMetaStoreSession ms,
      long securableCatalogId,
      long securableId) {

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
  public @NotNull LoadGrantsResult loadGrantsOnSecurable(
      @NotNull PolarisCallContext callCtx, long securableCatalogId, long securableId) {
    // get metastore we should be using
    PolarisMetaStoreSession ms = callCtx.getMetaStore();

    // need to run inside a read transaction
    return ms.runInReadTransaction(
        callCtx, () -> this.loadGrantsOnSecurable(callCtx, ms, securableCatalogId, securableId));
  }

  /** {@link #loadGrantsToGrantee(PolarisCallContext, long, long)} */
  public @NotNull LoadGrantsResult loadGrantsToGrantee(
      @NotNull PolarisCallContext callCtx,
      @NotNull PolarisMetaStoreSession ms,
      long granteeCatalogId,
      long granteeId) {

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
  public @NotNull LoadGrantsResult loadGrantsToGrantee(
      @NotNull PolarisCallContext callCtx, long granteeCatalogId, long granteeId) {
    // get metastore we should be using
    PolarisMetaStoreSession ms = callCtx.getMetaStore();

    // need to run inside a read transaction
    return ms.runInReadTransaction(
        callCtx, () -> this.loadGrantsToGrantee(callCtx, ms, granteeCatalogId, granteeId));
  }

  /** {@link PolarisMetaStoreManager#loadEntitiesChangeTracking(PolarisCallContext, List)} */
  private @NotNull ChangeTrackingResult loadEntitiesChangeTracking(
      @NotNull PolarisCallContext callCtx,
      @NotNull PolarisMetaStoreSession ms,
      @NotNull List<PolarisEntityId> entityIds) {
    List<PolarisChangeTrackingVersions> changeTracking =
        ms.lookupEntityVersions(callCtx, entityIds);
    return new ChangeTrackingResult(changeTracking);
  }

  /** {@inheritDoc} */
  @Override
  public @NotNull ChangeTrackingResult loadEntitiesChangeTracking(
      @NotNull PolarisCallContext callCtx, @NotNull List<PolarisEntityId> entityIds) {
    // get metastore we should be using
    PolarisMetaStoreSession ms = callCtx.getMetaStore();

    // need to run inside a read transaction
    return ms.runInReadTransaction(
        callCtx, () -> this.loadEntitiesChangeTracking(callCtx, ms, entityIds));
  }

  /** Refer to {@link #loadEntity(PolarisCallContext, long, long)} */
  private @NotNull EntityResult loadEntity(
      @NotNull PolarisCallContext callCtx,
      @NotNull PolarisMetaStoreSession ms,
      long entityCatalogId,
      long entityId) {
    // this is an easy one
    PolarisBaseEntity entity = ms.lookupEntity(callCtx, entityCatalogId, entityId);
    return (entity != null)
        ? new EntityResult(entity)
        : new EntityResult(BaseResult.ReturnStatus.ENTITY_NOT_FOUND, null);
  }

  /** {@inheritDoc} */
  @Override
  public @NotNull EntityResult loadEntity(
      @NotNull PolarisCallContext callCtx, long entityCatalogId, long entityId) {
    // get metastore we should be using
    PolarisMetaStoreSession ms = callCtx.getMetaStore();

    // need to run inside a read transaction
    return ms.runInReadTransaction(
        callCtx, () -> this.loadEntity(callCtx, ms, entityCatalogId, entityId));
  }

  /** Refer to {@link #loadTasks(PolarisCallContext, String, int)} */
  private @NotNull EntitiesResult loadTasks(
      @NotNull PolarisCallContext callCtx,
      @NotNull PolarisMetaStoreSession ms,
      String executorId,
      int limit) {

    // find all available tasks
    List<PolarisBaseEntity> availableTasks =
        ms.listActiveEntities(
            callCtx,
            PolarisEntityConstants.getRootEntityId(),
            PolarisEntityConstants.getRootEntityId(),
            PolarisEntityType.TASK,
            limit,
            entity -> {
              PolarisObjectMapperUtil.TaskExecutionState taskState =
                  PolarisObjectMapperUtil.parseTaskState(entity);
              long taskAgeTimeout =
                  callCtx
                      .getConfigurationStore()
                      .getConfiguration(
                          callCtx,
                          PolarisTaskConstants.TASK_TIMEOUT_MILLIS_CONFIG,
                          PolarisTaskConstants.TASK_TIMEOUT_MILLIS);
              return taskState == null
                  || taskState.executor == null
                  || callCtx.getClock().millis() - taskState.lastAttemptStartTime > taskAgeTimeout;
            },
            Function.identity());

    availableTasks.forEach(
        task -> {
          Map<String, String> properties =
              PolarisObjectMapperUtil.deserializeProperties(callCtx, task.getProperties());
          properties.put(PolarisTaskConstants.LAST_ATTEMPT_EXECUTOR_ID, executorId);
          properties.put(
              PolarisTaskConstants.LAST_ATTEMPT_START_TIME,
              String.valueOf(callCtx.getClock().millis()));
          properties.put(
              PolarisTaskConstants.ATTEMPT_COUNT,
              String.valueOf(
                  Integer.parseInt(properties.getOrDefault(PolarisTaskConstants.ATTEMPT_COUNT, "0"))
                      + 1));
          task.setEntityVersion(task.getEntityVersion() + 1);
          task.setProperties(PolarisObjectMapperUtil.serializeProperties(callCtx, properties));
          writeEntity(callCtx, ms, task, false);
        });
    return new EntitiesResult(availableTasks);
  }

  @Override
  public @NotNull EntitiesResult loadTasks(
      @NotNull PolarisCallContext callCtx, String executorId, int limit) {
    PolarisMetaStoreSession ms = callCtx.getMetaStore();
    return ms.runInTransaction(callCtx, () -> this.loadTasks(callCtx, ms, executorId, limit));
  }

  /** {@inheritDoc} */
  @Override
  public @NotNull ScopedCredentialsResult getSubscopedCredsForEntity(
      @NotNull PolarisCallContext callCtx,
      long catalogId,
      long entityId,
      boolean allowListOperation,
      @NotNull Set<String> allowedReadLocations,
      @NotNull Set<String> allowedWriteLocations) {

    // get meta store session we should be using
    PolarisMetaStoreSession ms = callCtx.getMetaStore();
    callCtx
        .getDiagServices()
        .check(
            !allowedReadLocations.isEmpty() || !allowedWriteLocations.isEmpty(),
            "allowed_locations_to_subscope_is_required");

    // reload the entity, error out if not found
    EntityResult reloadedEntity = loadEntity(callCtx, catalogId, entityId);
    if (reloadedEntity.getReturnStatus() != BaseResult.ReturnStatus.SUCCESS) {
      return new ScopedCredentialsResult(
          reloadedEntity.getReturnStatus(), reloadedEntity.getExtraInformation());
    }

    // get storage integration
    PolarisStorageIntegration<PolarisStorageConfigurationInfo> storageIntegration =
        ms.loadPolarisStorageIntegration(callCtx, reloadedEntity.getEntity());

    // cannot be null
    callCtx
        .getDiagServices()
        .checkNotNull(
            storageIntegration,
            "storage_integration_not_exists",
            "catalogId={}, entityId={}",
            catalogId,
            entityId);

    PolarisStorageConfigurationInfo storageConfigurationInfo =
        readStorageConfiguration(callCtx, reloadedEntity.getEntity());
    try {
      EnumMap<PolarisCredentialProperty, String> creds =
          storageIntegration.getSubscopedCreds(
              callCtx.getDiagServices(),
              storageConfigurationInfo,
              allowListOperation,
              allowedReadLocations,
              allowedWriteLocations);
      return new ScopedCredentialsResult(creds);
    } catch (Exception ex) {
      return new ScopedCredentialsResult(
          BaseResult.ReturnStatus.SUBSCOPE_CREDS_ERROR, ex.getMessage());
    }
  }

  /** {@inheritDoc} */
  @Override
  public @NotNull ValidateAccessResult validateAccessToLocations(
      @NotNull PolarisCallContext callCtx,
      long catalogId,
      long entityId,
      @NotNull Set<PolarisStorageActions> actions,
      @NotNull Set<String> locations) {
    // get meta store we should be using
    PolarisMetaStoreSession ms = callCtx.getMetaStore();
    callCtx
        .getDiagServices()
        .check(
            !actions.isEmpty() && !locations.isEmpty(),
            "locations_and_operations_privileges_are_required");
    // reload the entity, error out if not found
    EntityResult reloadedEntity = loadEntity(callCtx, catalogId, entityId);
    if (reloadedEntity.getReturnStatus() != BaseResult.ReturnStatus.SUCCESS) {
      return new ValidateAccessResult(
          reloadedEntity.getReturnStatus(), reloadedEntity.getExtraInformation());
    }

    // get storage integration, expect not null
    PolarisStorageIntegration<PolarisStorageConfigurationInfo> storageIntegration =
        ms.loadPolarisStorageIntegration(callCtx, reloadedEntity.getEntity());
    callCtx
        .getDiagServices()
        .checkNotNull(
            storageIntegration,
            "storage_integration_not_exists",
            "catalogId={}, entityId={}",
            catalogId,
            entityId);

    // validate access
    PolarisStorageConfigurationInfo storageConfigurationInfo =
        readStorageConfiguration(callCtx, reloadedEntity.getEntity());
    Map<String, String> validateLocationAccess =
        storageIntegration
            .validateAccessToLocations(storageConfigurationInfo, actions, locations)
            .entrySet()
            .stream()
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey,
                    e -> PolarisObjectMapperUtil.serialize(callCtx, e.getValue())));

    // done, return result
    return new ValidateAccessResult(validateLocationAccess);
  }

  public static PolarisStorageConfigurationInfo readStorageConfiguration(
      @NotNull PolarisCallContext callCtx, PolarisBaseEntity reloadedEntity) {
    Map<String, String> propMap =
        PolarisObjectMapperUtil.deserializeProperties(
            callCtx, reloadedEntity.getInternalProperties());
    String storageConfigInfoStr =
        propMap.get(PolarisEntityConstants.getStorageConfigInfoPropertyName());

    callCtx
        .getDiagServices()
        .check(
            storageConfigInfoStr != null,
            "missing_storage_configuration_info",
            "catalogId={}, entityId={}",
            reloadedEntity.getCatalogId(),
            reloadedEntity.getId());
    return PolarisStorageConfigurationInfo.deserialize(
        callCtx.getDiagServices(), storageConfigInfoStr);
  }

  /**
   * Get the internal property map for an entity
   *
   * @param callCtx the polaris call context
   * @param entity the target entity
   * @return a map of string representing the internal properties
   */
  public Map<String, String> getInternalPropertyMap(
      @NotNull PolarisCallContext callCtx, @NotNull PolarisBaseEntity entity) {
    String internalPropStr = entity.getInternalProperties();
    Map<String, String> res = new HashMap<>();
    if (internalPropStr == null) {
      return res;
    }
    return deserializeProperties(callCtx, internalPropStr);
  }

  /** {@link #loadCachedEntryById(PolarisCallContext, long, long)} */
  private @NotNull CachedEntryResult loadCachedEntryById(
      @NotNull PolarisCallContext callCtx,
      @NotNull PolarisMetaStoreSession ms,
      long entityCatalogId,
      long entityId) {

    // load that entity
    PolarisBaseEntity entity = ms.lookupEntity(callCtx, entityCatalogId, entityId);

    // if entity not found, return null
    if (entity == null) {
      return new CachedEntryResult(BaseResult.ReturnStatus.ENTITY_NOT_FOUND, null);
    }

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
    return new CachedEntryResult(entity, entity.getGrantRecordsVersion(), grantRecords);
  }

  /** {@inheritDoc} */
  @Override
  public @NotNull CachedEntryResult loadCachedEntryById(
      @NotNull PolarisCallContext callCtx, long entityCatalogId, long entityId) {
    // get metastore we should be using
    PolarisMetaStoreSession ms = callCtx.getMetaStore();

    // need to run inside a read transaction
    return ms.runInReadTransaction(
        callCtx, () -> this.loadCachedEntryById(callCtx, ms, entityCatalogId, entityId));
  }

  /** {@link #loadCachedEntryById(PolarisCallContext, long, long)} */
  private @NotNull CachedEntryResult loadCachedEntryByName(
      @NotNull PolarisCallContext callCtx,
      @NotNull PolarisMetaStoreSession ms,
      long entityCatalogId,
      long parentId,
      @NotNull PolarisEntityType entityType,
      @NotNull String entityName) {

    // load that entity
    PolarisEntitiesActiveKey entityActiveKey =
        new PolarisEntitiesActiveKey(entityCatalogId, parentId, entityType.getCode(), entityName);
    PolarisBaseEntity entity = this.lookupEntityByName(callCtx, ms, entityActiveKey);

    // null if entity not found
    if (entity == null) {
      return new CachedEntryResult(BaseResult.ReturnStatus.ENTITY_NOT_FOUND, null);
    }

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

    // return the result
    return new CachedEntryResult(entity, entity.getGrantRecordsVersion(), grantRecords);
  }

  /** {@inheritDoc} */
  @Override
  public @NotNull CachedEntryResult loadCachedEntryByName(
      @NotNull PolarisCallContext callCtx,
      long entityCatalogId,
      long parentId,
      @NotNull PolarisEntityType entityType,
      @NotNull String entityName) {
    // get metastore we should be using
    PolarisMetaStoreSession ms = callCtx.getMetaStore();

    // need to run inside a read transaction
    CachedEntryResult result =
        ms.runInReadTransaction(
            callCtx,
            () ->
                this.loadCachedEntryByName(
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
              PolarisEntitiesActiveKey serviceAdminRoleKey =
                  new PolarisEntitiesActiveKey(
                      0L,
                      0L,
                      PolarisEntityType.PRINCIPAL_ROLE.getCode(),
                      PolarisEntityConstants.getNameOfPrincipalServiceAdminRole());
              PolarisBaseEntity serviceAdminRole =
                  this.lookupEntityByName(callCtx, ms, serviceAdminRoleKey);
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
                  this.loadCachedEntryByName(
                      callCtx, ms, entityCatalogId, parentId, entityType, entityName));
    }
    return result;
  }

  /** {@inheritDoc} */
  private @NotNull CachedEntryResult refreshCachedEntity(
      @NotNull PolarisCallContext callCtx,
      @NotNull PolarisMetaStoreSession ms,
      int entityVersion,
      int entityGrantRecordsVersion,
      @NotNull PolarisEntityType entityType,
      long entityCatalogId,
      long entityId) {

    // load version information
    PolarisChangeTrackingVersions entityVersions =
        ms.lookupEntityVersions(callCtx, List.of(new PolarisEntityId(entityCatalogId, entityId)))
            .get(0);

    // if null, the entity has been purged
    if (entityVersions == null) {
      return new CachedEntryResult(BaseResult.ReturnStatus.ENTITY_NOT_FOUND, null);
    }

    // load the entity if something changed
    final PolarisBaseEntity entity;
    if (entityVersion != entityVersions.getEntityVersion()) {
      entity = ms.lookupEntity(callCtx, entityCatalogId, entityId);

      // if not found, return null
      if (entity == null) {
        return new CachedEntryResult(BaseResult.ReturnStatus.ENTITY_NOT_FOUND, null);
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
            new ArrayList<>(ms.loadAllGrantRecordsOnGrantee(callCtx, entityCatalogId, entityId));
        grantRecords.addAll(ms.loadAllGrantRecordsOnSecurable(callCtx, entityCatalogId, entityId));
      } else {
        grantRecords = ms.loadAllGrantRecordsOnSecurable(callCtx, entityCatalogId, entityId);
      }
    } else {
      grantRecords = null;
    }

    // return the result
    return new CachedEntryResult(entity, entityVersions.getGrantRecordsVersion(), grantRecords);
  }

  /** {@inheritDoc} */
  @Override
  public @NotNull CachedEntryResult refreshCachedEntity(
      @NotNull PolarisCallContext callCtx,
      int entityVersion,
      int entityGrantRecordsVersion,
      @NotNull PolarisEntityType entityType,
      long entityCatalogId,
      long entityId) {
    // get metastore we should be using
    PolarisMetaStoreSession ms = callCtx.getMetaStore();

    // need to run inside a read transaction
    return ms.runInReadTransaction(
        callCtx,
        () ->
            this.refreshCachedEntity(
                callCtx,
                ms,
                entityVersion,
                entityGrantRecordsVersion,
                entityType,
                entityCatalogId,
                entityId));
  }
}
