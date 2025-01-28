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
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.time.Clock;
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
import org.apache.polaris.core.PolarisConfigurationStore;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.context.Realm;
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
import org.apache.polaris.core.storage.PolarisCredentialProperty;
import org.apache.polaris.core.storage.PolarisStorageActions;
import org.apache.polaris.core.storage.PolarisStorageConfigurationInfo;
import org.apache.polaris.core.storage.PolarisStorageIntegration;
import org.jetbrains.annotations.NotNull;
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

  private final Realm realm;
  private final PolarisDiagnostics diagnostics;
  private final PolarisConfigurationStore configurationStore;
  private final Clock clock;

  public PolarisMetaStoreManagerImpl(
      Realm realm,
      PolarisDiagnostics diagnostics,
      PolarisConfigurationStore configurationStore,
      Clock clock) {
    this.realm = realm;
    this.diagnostics = diagnostics;
    this.configurationStore = configurationStore;
    this.clock = clock;
  }

  /**
   * Lookup an entity by its name
   *
   * @param ms meta store
   * @param entityActiveKey lookup key
   * @return the entity if it exists, null otherwise
   */
  private @Nullable PolarisBaseEntity lookupEntityByName(
      @Nonnull PolarisMetaStoreSession ms, @Nonnull PolarisEntitiesActiveKey entityActiveKey) {
    // ensure that the entity exists
    PolarisEntityActiveRecord entityActiveRecord = ms.lookupEntityActive(entityActiveKey);

    // if not found, return null
    if (entityActiveRecord == null) {
      return null;
    }

    // lookup the entity, should be there
    PolarisBaseEntity entity =
        ms.lookupEntity(entityActiveRecord.getCatalogId(), entityActiveRecord.getId());
    diagnostics.checkNotNull(
        entity, "unexpected_not_found_entity", "entityActiveRecord={}", entityActiveRecord);

    // return it now
    return entity;
  }

  /**
   * Write this entity to the meta store.
   *
   * @param ms meta store in read/write mode
   * @param entity entity to persist
   * @param writeToActive if true, write it to active
   */
  private void writeEntity(
      @Nonnull PolarisMetaStoreSession ms,
      @Nonnull PolarisBaseEntity entity,
      boolean writeToActive) {
    ms.writeToEntities(entity);
    ms.writeToEntitiesChangeTracking(entity);

    if (writeToActive) {
      ms.writeToEntitiesActive(entity);
    }
  }

  /**
   * Persist the specified new entity. Persist will write this entity in the ENTITIES, in the
   * ENTITIES_ACTIVE and finally in the ENTITIES_CHANGE_TRACKING tables
   *
   * @param ms meta store in read/write mode
   * @param entity entity we need a DPO for
   */
  private void persistNewEntity(
      @Nonnull PolarisMetaStoreSession ms, @Nonnull PolarisBaseEntity entity) {

    // validate the entity type and subtype
    diagnostics.checkNotNull(entity, "unexpected_null_entity");
    diagnostics.checkNotNull(entity.getName(), "unexpected_null_name", "entity={}", entity);
    PolarisEntityType type = PolarisEntityType.fromCode(entity.getTypeCode());
    diagnostics.checkNotNull(type, "unknown_type", "entity={}", entity);
    PolarisEntitySubType subType = PolarisEntitySubType.fromCode(entity.getSubTypeCode());
    diagnostics.checkNotNull(subType, "unexpected_null_subType", "entity={}", entity);
    diagnostics.check(
        subType.getParentType() == null || subType.getParentType() == type,
        "invalid_subtype",
        "type={} subType={}",
        type,
        subType);

    // if top-level entity, its parent should be the account
    diagnostics.check(
        !type.isTopLevel() || entity.getParentId() == PolarisEntityConstants.getRootEntityId(),
        "top_level_parent_should_be_account",
        "entity={}",
        entity);

    // id should not be null
    diagnostics.check(
        entity.getId() != 0 || type == PolarisEntityType.ROOT, "id_not_set", "entity={}", entity);

    // creation timestamp must be filled
    diagnostics.check(entity.getCreateTimestamp() != 0, "null_create_timestamp");

    // this is the first change
    entity.setLastUpdateTimestamp(entity.getCreateTimestamp());

    // set all other timestamps to 0
    entity.setDropTimestamp(0);
    entity.setPurgeTimestamp(0);
    entity.setToPurgeTimestamp(0);

    // write it
    this.writeEntity(ms, entity, true);
  }

  /**
   * Persist the specified entity after it has been changed. We will update the last changed time,
   * increment the entity version and persist it back to the ENTITIES and ENTITIES_CHANGE_TRACKING
   * tables
   *
   * @param ms meta store
   * @param entity the entity which has been changed
   * @return the entity with its version and lastUpdateTimestamp updated
   */
  private @Nonnull PolarisBaseEntity persistEntityAfterChange(
      @Nonnull PolarisMetaStoreSession ms, @Nonnull PolarisBaseEntity entity) {

    // validate the entity type and subtype
    diagnostics.checkNotNull(entity, "unexpected_null_entity");
    diagnostics.checkNotNull(entity.getName(), "unexpected_null_name", "entity={}", entity);
    PolarisEntityType type = entity.getType();
    diagnostics.checkNotNull(type, "unexpected_null_type", "entity={}", entity);
    PolarisEntitySubType subType = entity.getSubType();
    diagnostics.checkNotNull(subType, "unexpected_null_subType", "entity={}", entity);
    diagnostics.check(
        subType.getParentType() == null || subType.getParentType() == type,
        "invalid_subtype",
        "type={} subType={} entity={}",
        type,
        subType,
        entity);

    // entity should not have been dropped
    diagnostics.check(entity.getDropTimestamp() == 0, "entity_dropped", "entity={}", entity);

    // creation timestamp must be filled
    long createTimestamp = entity.getCreateTimestamp();
    diagnostics.check(createTimestamp != 0, "null_create_timestamp", "entity={}", entity);

    // ensure time is not moving backward...
    long now = System.currentTimeMillis();
    if (now < entity.getCreateTimestamp()) {
      now = entity.getCreateTimestamp() + 1;
    }

    // update last update timestamp and increment entity version
    entity.setLastUpdateTimestamp(now);
    entity.setEntityVersion(entity.getEntityVersion() + 1);

    // persist it to the various slices
    this.writeEntity(ms, entity, false);

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
   * @param ms meta store
   * @param entity the entity being dropped
   */
  private void dropEntity(@Nonnull PolarisMetaStoreSession ms, @Nonnull PolarisBaseEntity entity) {

    // validate the entity type and subtype
    diagnostics.checkNotNull(entity, "unexpected_null_dpo");
    diagnostics.checkNotNull(entity.getName(), "unexpected_null_name");

    // creation timestamp must be filled
    diagnostics.check(entity.getDropTimestamp() == 0, "already_dropped");

    // delete it from active slice
    ms.deleteFromEntitiesActive(entity);

    // for now drop all entities synchronously
    if (USE_SYNCHRONOUS_DROP) {
      // use synchronous drop

      // delete ALL grant records to (if the entity is a grantee) and from that entity
      final List<PolarisGrantRecord> grantsOnGrantee =
          (entity.getType().isGrantee())
              ? ms.loadAllGrantRecordsOnGrantee(entity.getCatalogId(), entity.getId())
              : List.of();
      final List<PolarisGrantRecord> grantsOnSecurable =
          ms.loadAllGrantRecordsOnSecurable(entity.getCatalogId(), entity.getId());
      ms.deleteAllEntityGrantRecords(entity, grantsOnGrantee, grantsOnSecurable);

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
      List<PolarisBaseEntity> entities = ms.lookupEntities(new ArrayList<>(entityIdsGrantChanged));
      for (PolarisBaseEntity entityGrantChanged : entities) {
        entityGrantChanged.setGrantRecordsVersion(entityGrantChanged.getGrantRecordsVersion() + 1);
        ms.writeToEntities(entityGrantChanged);
        ms.writeToEntitiesChangeTracking(entityGrantChanged);
      }

      // remove the entity being dropped now
      ms.deleteFromEntities(entity);
      ms.deleteFromEntitiesChangeTracking(entity);

      // if it is a principal, we also need to drop the secrets
      if (entity.getType() == PolarisEntityType.PRINCIPAL) {
        // get internal properties
        Map<String, String> properties = this.deserializeProperties(entity.getInternalProperties());

        // get client_id
        String clientId = properties.get(PolarisEntityConstants.getClientIdPropertyName());

        // delete it from the secret slice
        ms.deletePrincipalSecrets(clientId, entity.getId());
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
      ms.writeToEntities(entity);
      ms.writeToEntitiesDropped(entity);
      ms.writeToEntitiesChangeTracking(entity);
    }
  }

  /**
   * Create and persist a new grant record. This will at the same time invalidate the grant records
   * of the grantee and the securable if the grantee is a catalog role
   *
   * @param ms meta store in read/write mode
   * @param securable securable
   * @param grantee grantee, either a catalog role, a principal role or a principal
   * @param priv privilege
   * @return new grant record which was created and persisted
   */
  private @Nonnull PolarisGrantRecord persistNewGrantRecord(
      @Nonnull PolarisMetaStoreSession ms,
      @Nonnull PolarisEntityCore securable,
      @Nonnull PolarisEntityCore grantee,
      @Nonnull PolarisPrivilege priv) {

    // validate non null arguments
    diagnostics.checkNotNull(securable, "unexpected_null_securable");
    diagnostics.checkNotNull(grantee, "unexpected_null_grantee");
    diagnostics.checkNotNull(priv, "unexpected_null_priv");

    // ensure that this entity is indeed a grantee like entity
    diagnostics.check(
        grantee.getType().isGrantee(), "entity_must_be_grantee", "entity={}", grantee);

    // create new grant record
    PolarisGrantRecord grantRecord =
        new PolarisGrantRecord(
            securable.getCatalogId(),
            securable.getId(),
            grantee.getCatalogId(),
            grantee.getId(),
            priv.getCode());

    // persist the new grant
    ms.writeToGrantRecords(grantRecord);

    // load the grantee (either a catalog/principal role or a principal) and increment its grants
    // version
    PolarisBaseEntity granteeEntity = ms.lookupEntity(grantee.getCatalogId(), grantee.getId());
    diagnostics.checkNotNull(granteeEntity, "grantee_not_found", "grantee={}", grantee);

    // grants have changed, we need to bump-up the grants version
    granteeEntity.setGrantRecordsVersion(granteeEntity.getGrantRecordsVersion() + 1);
    this.writeEntity(ms, granteeEntity, false);

    // we also need to invalidate the grants on that securable so that we can reload them.
    // load the securable and increment its grants version
    PolarisBaseEntity securableEntity =
        ms.lookupEntity(securable.getCatalogId(), securable.getId());
    diagnostics.checkNotNull(securableEntity, "securable_not_found", "securable={}", securable);

    // grants have changed, we need to bump-up the grants version
    securableEntity.setGrantRecordsVersion(securableEntity.getGrantRecordsVersion() + 1);
    this.writeEntity(ms, securableEntity, false);

    // done, return the new grant record
    return grantRecord;
  }

  /**
   * Delete the specified grant record from the GRANT_RECORDS table. This will at the same time
   * invalidate the grant records of the grantee and the securable if the grantee is a role
   *
   * @param ms meta store
   * @param securable the securable entity
   * @param grantee the grantee entity
   * @param grantRecord the grant record to remove, which was read in the same transaction
   */
  private void revokeGrantRecord(
      @Nonnull PolarisMetaStoreSession ms,
      @Nonnull PolarisEntityCore securable,
      @Nonnull PolarisEntityCore grantee,
      @Nonnull PolarisGrantRecord grantRecord) {

    // validate securable
    diagnostics.check(
        securable.getCatalogId() == grantRecord.getSecurableCatalogId()
            && securable.getId() == grantRecord.getSecurableId(),
        "securable_mismatch",
        "securable={} grantRec={}",
        securable,
        grantRecord);

    // validate grantee
    diagnostics.check(
        grantee.getCatalogId() == grantRecord.getGranteeCatalogId()
            && grantee.getId() == grantRecord.getGranteeId(),
        "grantee_mismatch",
        "grantee={} grantRec={}",
        grantee,
        grantRecord);

    // ensure the grantee is really a grantee
    diagnostics.check(grantee.getType().isGrantee(), "not_a_grantee", "grantee={}", grantee);

    // remove that grant
    ms.deleteFromGrantRecords(grantRecord);

    // load the grantee and increment its grants version
    PolarisBaseEntity refreshGrantee = ms.lookupEntity(grantee.getCatalogId(), grantee.getId());
    diagnostics.checkNotNull(
        refreshGrantee, "missing_grantee", "grantRecord={} grantee={}", grantRecord, grantee);

    // grants have changed, we need to bump-up the grants version
    refreshGrantee.setGrantRecordsVersion(refreshGrantee.getGrantRecordsVersion() + 1);
    this.writeEntity(ms, refreshGrantee, false);

    // we also need to invalidate the grants on that securable so that we can reload them.
    // load the securable and increment its grants version
    PolarisBaseEntity refreshSecurable =
        ms.lookupEntity(securable.getCatalogId(), securable.getId());
    diagnostics.checkNotNull(
        refreshSecurable,
        "missing_securable",
        "grantRecord={} securable={}",
        grantRecord,
        securable);

    // grants have changed, we need to bump-up the grants version
    refreshSecurable.setGrantRecordsVersion(refreshSecurable.getGrantRecordsVersion() + 1);
    this.writeEntity(ms, refreshSecurable, false);
  }

  /**
   * Create a new catalog. This not only creates the new catalog entity but also the initial admin
   * role required to admin this catalog.
   *
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
  private @Nonnull CreateCatalogResult doCreateCatalog(
      @Nonnull PolarisMetaStoreSession ms,
      @Nonnull PolarisBaseEntity catalog,
      @Nullable PolarisStorageIntegration<?> integration,
      @Nonnull List<PolarisEntityCore> principalRoles) {
    // validate input
    diagnostics.checkNotNull(catalog, "unexpected_null_catalog");

    // check if that catalog has already been created
    PolarisBaseEntity refreshCatalog = ms.lookupEntity(catalog.getCatalogId(), catalog.getId());

    // if found, probably a retry, simply return the previously created catalog
    if (refreshCatalog != null) {
      // if found, ensure it is indeed a catalog
      diagnostics.check(
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
      PolarisBaseEntity catalogAdminRole = this.lookupEntityByName(ms, adminRoleKey);

      // if found, ensure not null
      diagnostics.checkNotNull(
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
    PolarisEntityActiveRecord otherCatalogRecord = ms.lookupEntityActive(catalogNameKey);

    // if it exists, this is an error, the client should retry
    if (otherCatalogRecord != null) {
      return new CreateCatalogResult(BaseResult.ReturnStatus.ENTITY_ALREADY_EXISTS, null);
    }

    ms.persistStorageIntegrationIfNeeded(catalog, integration);

    // now create and persist new catalog entity
    this.persistNewEntity(ms, catalog);

    // create the catalog admin role for this new catalog
    long adminRoleId = ms.generateNewId();
    PolarisBaseEntity adminRole =
        new PolarisBaseEntity(
            catalog.getId(),
            adminRoleId,
            PolarisEntityType.CATALOG_ROLE,
            PolarisEntitySubType.NULL_SUBTYPE,
            catalog.getId(),
            PolarisEntityConstants.getNameOfCatalogAdminRole());
    this.persistNewEntity(ms, adminRole);

    // grant the catalog admin role access-management on the catalog
    this.persistNewGrantRecord(ms, catalog, adminRole, PolarisPrivilege.CATALOG_MANAGE_ACCESS);

    // grant the catalog admin role metadata-management on the catalog; this one
    // is revocable
    this.persistNewGrantRecord(ms, catalog, adminRole, PolarisPrivilege.CATALOG_MANAGE_METADATA);

    // immediately assign its catalog_admin role
    if (principalRoles.isEmpty()) {
      // lookup service admin role, should exist
      PolarisEntitiesActiveKey serviceAdminRoleKey =
          new PolarisEntitiesActiveKey(
              PolarisEntityConstants.getNullId(),
              PolarisEntityConstants.getRootEntityId(),
              PolarisEntityType.PRINCIPAL_ROLE.getCode(),
              PolarisEntityConstants.getNameOfPrincipalServiceAdminRole());
      PolarisBaseEntity serviceAdminRole = this.lookupEntityByName(ms, serviceAdminRoleKey);
      diagnostics.checkNotNull(serviceAdminRole, "missing_service_admin_role");
      this.persistNewGrantRecord(
          ms, adminRole, serviceAdminRole, PolarisPrivilege.CATALOG_ROLE_USAGE);
    } else {
      // grant to each principal role usage on its catalog_admin role
      for (PolarisEntityCore principalRole : principalRoles) {
        // validate not null and really a principal role
        diagnostics.checkNotNull(principalRole, "null principal role");
        diagnostics.check(
            principalRole.getTypeCode() == PolarisEntityType.PRINCIPAL_ROLE.getCode(),
            "not_principal_role",
            "type={}",
            principalRole.getType());

        // grant usage on that catalog admin role to this principal
        this.persistNewGrantRecord(
            ms, adminRole, principalRole, PolarisPrivilege.CATALOG_ROLE_USAGE);
      }
    }

    // success, return the two entities
    return new CreateCatalogResult(catalog, adminRole);
  }

  /**
   * Bootstrap Polaris catalog service
   *
   * @param ms meta store in read/write mode
   */
  private void doBootstrapPolarisService(@Nonnull PolarisMetaStoreSession ms) {

    // Create a root container entity that can represent the securable for any top-level grants.
    PolarisBaseEntity rootContainer =
        new PolarisBaseEntity(
            PolarisEntityConstants.getNullId(),
            PolarisEntityConstants.getRootEntityId(),
            PolarisEntityType.ROOT,
            PolarisEntitySubType.NULL_SUBTYPE,
            PolarisEntityConstants.getRootEntityId(),
            PolarisEntityConstants.getRootContainerName());
    this.persistNewEntity(ms, rootContainer);

    // Now bootstrap the service by creating the root principal and the service_admin principal
    // role. The principal role will be granted to that root principal and the root catalog admin
    // of the root catalog will be granted to that principal role.
    long rootPrincipalId = ms.generateNewId();
    PolarisBaseEntity rootPrincipal =
        new PolarisBaseEntity(
            PolarisEntityConstants.getNullId(),
            rootPrincipalId,
            PolarisEntityType.PRINCIPAL,
            PolarisEntitySubType.NULL_SUBTYPE,
            PolarisEntityConstants.getRootEntityId(),
            PolarisEntityConstants.getRootPrincipalName());

    // create this principal
    this.doCreatePrincipal(ms, rootPrincipal);

    // now create the account admin principal role
    long serviceAdminPrincipalRoleId = ms.generateNewId();
    PolarisBaseEntity serviceAdminPrincipalRole =
        new PolarisBaseEntity(
            PolarisEntityConstants.getNullId(),
            serviceAdminPrincipalRoleId,
            PolarisEntityType.PRINCIPAL_ROLE,
            PolarisEntitySubType.NULL_SUBTYPE,
            PolarisEntityConstants.getRootEntityId(),
            PolarisEntityConstants.getNameOfPrincipalServiceAdminRole());
    this.persistNewEntity(ms, serviceAdminPrincipalRole);

    // we also need to grant usage on the account-admin principal to the principal
    this.persistNewGrantRecord(
        ms, serviceAdminPrincipalRole, rootPrincipal, PolarisPrivilege.PRINCIPAL_ROLE_USAGE);

    // grant SERVICE_MANAGE_ACCESS on the rootContainer to the serviceAdminPrincipalRole
    this.persistNewGrantRecord(
        ms, rootContainer, serviceAdminPrincipalRole, PolarisPrivilege.SERVICE_MANAGE_ACCESS);
  }

  /** {@inheritDoc} */
  @Override
  public @Nonnull BaseResult bootstrapPolarisService(@NotNull PolarisMetaStoreSession session) {
    // run operation in a read/write transaction
    session.runActionInTransaction(() -> this.doBootstrapPolarisService(session));

    // all good
    return new BaseResult(BaseResult.ReturnStatus.SUCCESS);
  }

  @Override
  public @Nonnull BaseResult purge(@NotNull PolarisMetaStoreSession session) {
    // get meta store we should be using

    // run operation in a read/write transaction
    LOGGER.warn("Deleting all metadata in the metastore...");
    session.runActionInTransaction(session::deleteAll);
    LOGGER.warn("Finished deleting all metadata in the metastore");

    // all good
    return new BaseResult(BaseResult.ReturnStatus.SUCCESS);
  }

  /**
   * See {@link #readEntityByName(PolarisMetaStoreSession, List, PolarisEntityType,
   * PolarisEntitySubType, String)}
   */
  private @Nonnull PolarisMetaStoreManager.EntityResult doReadEntityByName(
      @Nonnull PolarisMetaStoreSession ms,
      @Nullable List<PolarisEntityCore> catalogPath,
      @Nonnull PolarisEntityType entityType,
      @Nonnull PolarisEntitySubType entitySubType,
      @Nonnull String name) {
    // first resolve again the catalogPath to that entity
    PolarisEntityResolver resolver = new PolarisEntityResolver(diagnostics, ms, catalogPath);

    // return if we failed to resolve
    if (resolver.isFailure()) {
      return new EntityResult(BaseResult.ReturnStatus.CATALOG_PATH_CANNOT_BE_RESOLVED, null);
    }

    // now looking the entity by name
    PolarisEntitiesActiveKey entityActiveKey =
        new PolarisEntitiesActiveKey(
            resolver.getCatalogIdOrNull(), resolver.getParentId(), entityType.getCode(), name);
    PolarisBaseEntity entity = this.lookupEntityByName(ms, entityActiveKey);

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
  public @Nonnull PolarisMetaStoreManager.EntityResult readEntityByName(
      PolarisMetaStoreSession ms,
      @Nullable List<PolarisEntityCore> catalogPath,
      @Nonnull PolarisEntityType entityType,
      @Nonnull PolarisEntitySubType entitySubType,
      @Nonnull String name) {
    // get meta store we should be using

    // run operation in a read/write transaction
    return ms.runInReadTransaction(
        () -> doReadEntityByName(ms, catalogPath, entityType, entitySubType, name));
  }

  /**
   * See {@link #listEntities(PolarisMetaStoreSession, List, PolarisEntityType,
   * PolarisEntitySubType)}
   */
  private @Nonnull ListEntitiesResult doListEntities(
      @Nonnull PolarisMetaStoreSession ms,
      @Nullable List<PolarisEntityCore> catalogPath,
      @Nonnull PolarisEntityType entityType,
      @Nonnull PolarisEntitySubType entitySubType) {
    // first resolve again the catalogPath to that entity
    PolarisEntityResolver resolver = new PolarisEntityResolver(diagnostics, ms, catalogPath);

    // return if we failed to resolve
    if (resolver.isFailure()) {
      return new ListEntitiesResult(BaseResult.ReturnStatus.CATALOG_PATH_CANNOT_BE_RESOLVED, null);
    }

    // return list of active entities
    List<PolarisEntityActiveRecord> toreturnList =
        ms.listActiveEntities(resolver.getCatalogIdOrNull(), resolver.getParentId(), entityType);

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
  public @Nonnull ListEntitiesResult listEntities(
      PolarisMetaStoreSession ms,
      @Nullable List<PolarisEntityCore> catalogPath,
      @Nonnull PolarisEntityType entityType,
      @Nonnull PolarisEntitySubType entitySubType) {
    return ms.runInReadTransaction(
        () -> doListEntities(ms, catalogPath, entityType, entitySubType));
  }

  /** {@inheritDoc} */
  @Override
  public @Nonnull GenerateEntityIdResult generateNewEntityId(PolarisMetaStoreSession ms) {
    return new GenerateEntityIdResult(ms.generateNewId());
  }

  /**
   * Given the internal property as a map of key/value pairs, serialize it to a String
   *
   * @param properties a map of key/value pairs
   * @return a String, the JSON representation of the map
   */
  public String serializeProperties(Map<String, String> properties) {

    String jsonString = null;
    try {
      // Deserialize the JSON string to a Map<String, String>
      jsonString = MAPPER.writeValueAsString(properties);
    } catch (JsonProcessingException ex) {
      diagnostics.fail("got_json_processing_exception", "ex={}", ex);
    }

    return jsonString;
  }

  /**
   * Given the serialized properties, deserialize those to a {@code Map<String, String>}
   *
   * @param properties a JSON string representing the set of properties
   * @return a Map of string
   */
  public Map<String, String> deserializeProperties(String properties) {

    Map<String, String> retProperties = null;
    try {
      // Deserialize the JSON string to a Map<String, String>
      retProperties = MAPPER.readValue(properties, new TypeReference<>() {});
    } catch (JsonMappingException ex) {
      diagnostics.fail("got_json_mapping_exception", "ex={}", ex);
    } catch (JsonProcessingException ex) {
      diagnostics.fail("got_json_processing_exception", "ex={}", ex);
    }

    return retProperties;
  }

  /** {@link #createPrincipal(PolarisMetaStoreSession, PolarisBaseEntity)} */
  private @Nonnull CreatePrincipalResult doCreatePrincipal(
      @Nonnull PolarisMetaStoreSession ms, @Nonnull PolarisBaseEntity principal) {
    // validate input
    diagnostics.checkNotNull(principal, "unexpected_null_principal");

    // check if that catalog has already been created
    PolarisBaseEntity refreshPrincipal =
        ms.lookupEntity(principal.getCatalogId(), principal.getId());

    // if found, probably a retry, simply return the previously created principal
    if (refreshPrincipal != null) {
      // if found, ensure it is indeed a principal
      diagnostics.check(
          principal.getTypeCode() == PolarisEntityType.PRINCIPAL.getCode(),
          "not_a_principal",
          "principal={}",
          principal);

      // get internal properties
      Map<String, String> properties =
          this.deserializeProperties(refreshPrincipal.getInternalProperties());

      // get client_id
      String clientId = properties.get(PolarisEntityConstants.getClientIdPropertyName());

      // should not be null
      diagnostics.checkNotNull(
          clientId, "null_client_id", "properties={}", refreshPrincipal.getInternalProperties());
      // ensure non null and non empty
      diagnostics.check(
          !clientId.isEmpty(),
          "empty_client_id",
          "properties={}",
          refreshPrincipal.getInternalProperties());

      // get the main and secondary secrets for that client
      PolarisPrincipalSecrets principalSecrets = ms.loadPrincipalSecrets(clientId);

      // should not be null
      diagnostics.checkNotNull(
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
    PolarisEntityActiveRecord otherPrincipalRecord = ms.lookupEntityActive(principalNameKey);

    // if it exists, this is an error, the client should retry
    if (otherPrincipalRecord != null) {
      return new CreatePrincipalResult(BaseResult.ReturnStatus.ENTITY_ALREADY_EXISTS, null);
    }

    // generate new secrets for this principal
    PolarisPrincipalSecrets principalSecrets =
        ms.generateNewPrincipalSecrets(principal.getName(), principal.getId());

    // generate properties
    Map<String, String> internalProperties = getInternalPropertyMap(principal);
    internalProperties.put(
        PolarisEntityConstants.getClientIdPropertyName(), principalSecrets.getPrincipalClientId());

    // remember client id
    principal.setInternalProperties(this.serializeProperties(internalProperties));

    // now create and persist new catalog entity
    this.persistNewEntity(ms, principal);

    // success, return the two entities
    return new CreatePrincipalResult(principal, principalSecrets);
  }

  /** {@inheritDoc} */
  @Override
  public @Nonnull CreatePrincipalResult createPrincipal(
      PolarisMetaStoreSession ms, @Nonnull PolarisBaseEntity principal) {
    return ms.runInTransaction(() -> this.doCreatePrincipal(ms, principal));
  }

  /** See {@link #loadPrincipalSecrets(PolarisMetaStoreSession, String)} */
  private @Nullable PolarisPrincipalSecrets doLoadPrincipalSecrets(
      PolarisMetaStoreSession ms, @Nonnull String clientId) {
    return ms.loadPrincipalSecrets(clientId);
  }

  /** {@inheritDoc} */
  @Override
  public @Nonnull PrincipalSecretsResult loadPrincipalSecrets(
      PolarisMetaStoreSession ms, @Nonnull String clientId) {
    PolarisPrincipalSecrets secrets =
        ms.runInTransaction(() -> this.doLoadPrincipalSecrets(ms, clientId));

    return (secrets == null)
        ? new PrincipalSecretsResult(BaseResult.ReturnStatus.ENTITY_NOT_FOUND, null)
        : new PrincipalSecretsResult(secrets);
  }

  private @Nullable PolarisPrincipalSecrets doRotatePrincipalSecrets(
      @Nonnull PolarisDiagnostics diagnostics,
      @Nonnull PolarisMetaStoreSession ms,
      @Nonnull String clientId,
      long principalId,
      boolean reset,
      @Nonnull String oldSecretHash) {
    // if not found, the principal must have been dropped
    EntityResult loadEntityResult =
        doLoadEntity(ms, PolarisEntityConstants.getNullId(), principalId);
    if (loadEntityResult.getReturnStatus() != BaseResult.ReturnStatus.SUCCESS) {
      return null;
    }

    PolarisBaseEntity principal = loadEntityResult.getEntity();
    Map<String, String> internalProps =
        PolarisObjectMapperUtil.deserializeProperties(
            diagnostics,
            principal.getInternalProperties() == null ? "{}" : principal.getInternalProperties());

    boolean doReset =
        reset
            || internalProps.get(
                    PolarisEntityConstants.PRINCIPAL_CREDENTIAL_ROTATION_REQUIRED_STATE)
                != null;
    PolarisPrincipalSecrets secrets =
        ms.rotatePrincipalSecrets(clientId, principalId, doReset, oldSecretHash);

    if (reset
        && !internalProps.containsKey(
            PolarisEntityConstants.PRINCIPAL_CREDENTIAL_ROTATION_REQUIRED_STATE)) {
      internalProps.put(
          PolarisEntityConstants.PRINCIPAL_CREDENTIAL_ROTATION_REQUIRED_STATE, "true");
      principal.setInternalProperties(
          PolarisObjectMapperUtil.serializeProperties(diagnostics, internalProps));
      principal.setEntityVersion(principal.getEntityVersion() + 1);
      writeEntity(ms, principal, true);
    } else if (internalProps.containsKey(
        PolarisEntityConstants.PRINCIPAL_CREDENTIAL_ROTATION_REQUIRED_STATE)) {
      internalProps.remove(PolarisEntityConstants.PRINCIPAL_CREDENTIAL_ROTATION_REQUIRED_STATE);
      principal.setInternalProperties(
          PolarisObjectMapperUtil.serializeProperties(diagnostics, internalProps));
      principal.setEntityVersion(principal.getEntityVersion() + 1);
      writeEntity(ms, principal, true);
    }
    return secrets;
  }

  /** {@inheritDoc} */
  @Override
  public @Nonnull PrincipalSecretsResult rotatePrincipalSecrets(
      @Nonnull PolarisMetaStoreSession ms,
      @Nonnull String clientId,
      long principalId,
      boolean reset,
      @Nonnull String oldSecretHash) {
    // get metastore we should be using

    // need to run inside a read/write transaction
    PolarisPrincipalSecrets secrets =
        ms.runInTransaction(
            () ->
                this.doRotatePrincipalSecrets(
                    diagnostics, ms, clientId, principalId, reset, oldSecretHash));

    return (secrets == null)
        ? new PrincipalSecretsResult(BaseResult.ReturnStatus.ENTITY_NOT_FOUND, null)
        : new PrincipalSecretsResult(secrets);
  }

  /** {@inheritDoc} */
  @Override
  public @Nonnull CreateCatalogResult createCatalog(
      @Nonnull PolarisMetaStoreSession ms,
      @Nonnull PolarisBaseEntity catalog,
      @Nonnull List<PolarisEntityCore> principalRoles) {
    Map<String, String> internalProp = getInternalPropertyMap(catalog);
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
              catalog.getCatalogId(),
              catalog.getId(),
              PolarisStorageConfigurationInfo.deserialize(diagnostics, storageConfigInfoStr));
    } else {
      integration = null;
    }
    // need to run inside a read/write transaction
    return ms.runInTransaction(
        () -> this.doCreateCatalog(ms, catalog, integration, principalRoles));
  }

  /** {@link #createEntityIfNotExists(PolarisMetaStoreSession, List, PolarisBaseEntity)} */
  private @Nonnull EntityResult doCreateEntityIfNotExists(
      @Nonnull PolarisMetaStoreSession ms,
      @Nullable List<PolarisEntityCore> catalogPath,
      @Nonnull PolarisBaseEntity entity) {

    // entity cannot be null
    diagnostics.checkNotNull(entity, "unexpected_null_entity");

    // entity name must be specified
    diagnostics.checkNotNull(entity.getName(), "unexpected_null_entity_name");

    // first, check if the entity has already been created, in which case we will simply return it
    PolarisBaseEntity entityFound = ms.lookupEntity(entity.getCatalogId(), entity.getId());
    if (entityFound != null) {
      // probably the client retried, simply return it
      return new EntityResult(entityFound);
    }

    // first resolve again the catalogPath
    PolarisEntityResolver resolver = new PolarisEntityResolver(diagnostics, ms, catalogPath);

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
    PolarisEntityActiveRecord entityActiveRecord = ms.lookupEntityActive(entityActiveKey);
    if (entityActiveRecord != null) {
      return new EntityResult(
          BaseResult.ReturnStatus.ENTITY_ALREADY_EXISTS, entityActiveRecord.getSubTypeCode());
    }

    // persist that new entity
    this.persistNewEntity(ms, entity);

    // done, return that newly created entity
    return new EntityResult(entity);
  }

  /** {@inheritDoc} */
  @Override
  public @Nonnull EntityResult createEntityIfNotExists(
      PolarisMetaStoreSession ms,
      @Nullable List<PolarisEntityCore> catalogPath,
      @Nonnull PolarisBaseEntity entity) {
    // get metastore we should be using

    // need to run inside a read/write transaction
    return ms.runInTransaction(() -> this.doCreateEntityIfNotExists(ms, catalogPath, entity));
  }

  @Override
  public @Nonnull EntitiesResult createEntitiesIfNotExist(
      PolarisMetaStoreSession ms,
      @Nullable List<PolarisEntityCore> catalogPath,
      @Nonnull List<? extends PolarisBaseEntity> entities) {
    // get metastore we should be using

    // need to run inside a read/write transaction
    return ms.runInTransaction(
        () -> {
          List<PolarisBaseEntity> createdEntities = new ArrayList<>(entities.size());
          for (PolarisBaseEntity entity : entities) {
            EntityResult entityCreateResult = doCreateEntityIfNotExists(ms, catalogPath, entity);
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
   * See {@link #updateEntityPropertiesIfNotChanged(PolarisMetaStoreSession, List,
   * PolarisBaseEntity)}
   */
  private @Nonnull EntityResult doUpdateEntityPropertiesIfNotChanged(
      @Nonnull PolarisMetaStoreSession ms,
      @Nullable List<PolarisEntityCore> catalogPath,
      @Nonnull PolarisBaseEntity entity) {
    // entity cannot be null
    diagnostics.checkNotNull(entity, "unexpected_null_entity");

    // re-resolve everything including that entity
    PolarisEntityResolver resolver =
        new PolarisEntityResolver(diagnostics, ms, catalogPath, entity);

    // if resolution failed, return false
    if (resolver.isFailure()) {
      return new EntityResult(BaseResult.ReturnStatus.CATALOG_PATH_CANNOT_BE_RESOLVED, null);
    }

    // lookup the entity, cannot be null
    PolarisBaseEntity entityRefreshed = ms.lookupEntity(entity.getCatalogId(), entity.getId());
    diagnostics.checkNotNull(entityRefreshed, "unexpected_entity_not_found", "entity={}", entity);

    // check that the version of the entity has not changed at all to avoid concurrent updates
    if (entityRefreshed.getEntityVersion() != entity.getEntityVersion()) {
      return new EntityResult(BaseResult.ReturnStatus.TARGET_ENTITY_CONCURRENTLY_MODIFIED, null);
    }

    // update the two properties
    entityRefreshed.setInternalProperties(entity.getInternalProperties());
    entityRefreshed.setProperties(entity.getProperties());

    // persist this entity after changing it. This will update the version and update the last
    // updated time. Because the entity version is changed, we will update the change tracking table
    PolarisBaseEntity persistedEntity = this.persistEntityAfterChange(ms, entityRefreshed);
    return new EntityResult(persistedEntity);
  }

  /** {@inheritDoc} */
  @Override
  public @Nonnull EntityResult updateEntityPropertiesIfNotChanged(
      PolarisMetaStoreSession ms,
      @Nullable List<PolarisEntityCore> catalogPath,
      @Nonnull PolarisBaseEntity entity) {
    return ms.runInTransaction(
        () -> this.doUpdateEntityPropertiesIfNotChanged(ms, catalogPath, entity));
  }

  /** See {@link #updateEntitiesPropertiesIfNotChanged(PolarisMetaStoreSession, List)} */
  private @Nonnull EntitiesResult doUpdateEntitiesPropertiesIfNotChanged(
      @Nonnull PolarisMetaStoreSession ms, @Nonnull List<EntityWithPath> entities) {
    // ensure that the entities list is not null
    diagnostics.checkNotNull(entities, "unexpected_null_entities");

    // list of all updated entities
    List<PolarisBaseEntity> updatedEntities = new ArrayList<>(entities.size());

    // iterate over the list and update each, one at a time
    for (EntityWithPath entityWithPath : entities) {
      // update that entity, abort if it fails
      EntityResult updatedEntityResult =
          this.doUpdateEntityPropertiesIfNotChanged(
              ms, entityWithPath.getCatalogPath(), entityWithPath.getEntity());

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
  public @Nonnull EntitiesResult updateEntitiesPropertiesIfNotChanged(
      PolarisMetaStoreSession ms, @Nonnull List<EntityWithPath> entities) {
    return ms.runInTransaction(() -> this.doUpdateEntitiesPropertiesIfNotChanged(ms, entities));
  }

  /**
   * See {@link PolarisMetaStoreManager#renameEntity(PolarisMetaStoreSession, List,
   * PolarisEntityCore, List, PolarisEntity)}
   */
  private @Nonnull EntityResult doRenameEntity(
      @Nonnull PolarisMetaStoreSession ms,
      @Nullable List<PolarisEntityCore> catalogPath,
      @Nonnull PolarisEntityCore entityToRename,
      @Nullable List<PolarisEntityCore> newCatalogPath,
      @Nonnull PolarisBaseEntity renamedEntity) {

    // entity and new name cannot be null
    diagnostics.checkNotNull(entityToRename, "unexpected_null_entityToRename");
    diagnostics.checkNotNull(renamedEntity, "unexpected_null_renamedEntity");

    // if a new catalog path is specified (i.e. re-parent operation), a catalog path should be
    // specified too
    diagnostics.check(
        (newCatalogPath == null) || (catalogPath != null),
        "newCatalogPath_specified_without_catalogPath");

    // null is shorthand for saying the path isn't changing
    if (newCatalogPath == null) {
      newCatalogPath = catalogPath;
    }

    // re-resolve everything including that entity
    PolarisEntityResolver resolver =
        new PolarisEntityResolver(diagnostics, ms, catalogPath, entityToRename);

    // if resolution failed, return false
    if (resolver.isFailure()) {
      return new EntityResult(BaseResult.ReturnStatus.ENTITY_CANNOT_BE_RESOLVED, null);
    }

    // find the entity to rename
    PolarisBaseEntity refreshEntityToRename =
        ms.lookupEntity(entityToRename.getCatalogId(), entityToRename.getId());

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
      resolver = new PolarisEntityResolver(diagnostics, ms, newCatalogPath);

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
    PolarisEntityActiveRecord entityActiveRecord = ms.lookupEntityActive(entityActiveKey);
    if (entityActiveRecord != null) {
      return new EntityResult(
          BaseResult.ReturnStatus.ENTITY_ALREADY_EXISTS, entityActiveRecord.getSubTypeCode());
    }

    // all good, delete the existing entity from the active slice
    ms.deleteFromEntitiesActive(refreshEntityToRename);

    // change its name now
    refreshEntityToRename.setName(renamedEntity.getName());
    refreshEntityToRename.setProperties(renamedEntity.getProperties());
    refreshEntityToRename.setInternalProperties(renamedEntity.getInternalProperties());

    // re-parent if a new catalog path was specified
    if (newCatalogPath != null) {
      refreshEntityToRename.setParentId(resolver.getParentId());
    }

    // persist back to the active slice with its new name and parent
    ms.writeToEntitiesActive(refreshEntityToRename);

    // persist the entity after change. This wil update the lastUpdateTimestamp and bump up the
    // version
    PolarisBaseEntity renamedEntityToReturn =
        this.persistEntityAfterChange(ms, refreshEntityToRename);
    return new EntityResult(renamedEntityToReturn);
  }

  /** {@inheritDoc} */
  @Override
  public @Nonnull EntityResult renameEntity(
      PolarisMetaStoreSession ms,
      @Nullable List<PolarisEntityCore> catalogPath,
      @Nonnull PolarisEntityCore entityToRename,
      @Nullable List<PolarisEntityCore> newCatalogPath,
      @Nonnull PolarisEntity renamedEntity) {
    return ms.runInTransaction(
        () -> this.doRenameEntity(ms, catalogPath, entityToRename, newCatalogPath, renamedEntity));
  }

  /**
   * See
   *
   * <p>{@link #dropEntityIfExists(PolarisMetaStoreSession, List, PolarisEntityCore, Map, boolean)}
   */
  private @Nonnull DropEntityResult doDropEntityIfExists(
      @Nonnull PolarisMetaStoreSession ms,
      @Nullable List<PolarisEntityCore> catalogPath,
      @Nonnull PolarisEntityCore entityToDrop,
      @Nullable Map<String, String> cleanupProperties,
      boolean cleanup) {
    // entity cannot be null
    diagnostics.checkNotNull(entityToDrop, "unexpected_null_entity");

    // re-resolve everything including that entity
    PolarisEntityResolver resolver =
        new PolarisEntityResolver(diagnostics, ms, catalogPath, entityToDrop);

    // if resolution failed, return false
    if (resolver.isFailure()) {
      return new DropEntityResult(BaseResult.ReturnStatus.CATALOG_PATH_CANNOT_BE_RESOLVED, null);
    }

    // first find the entity to drop
    PolarisBaseEntity refreshEntityToDrop =
        ms.lookupEntity(entityToDrop.getCatalogId(), entityToDrop.getId());

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
      if (ms.hasChildren(PolarisEntityType.NAMESPACE, catalogId, catalogId)) {
        return new DropEntityResult(BaseResult.ReturnStatus.NAMESPACE_NOT_EMPTY, null);
      }

      // get the list of catalog roles, at most 2
      List<PolarisBaseEntity> catalogRoles =
          ms.listActiveEntities(
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
        this.dropEntity(ms, catalogRoles.get(0));
      }
    } else if (refreshEntityToDrop.getType() == PolarisEntityType.NAMESPACE) {
      if (ms.hasChildren(null, refreshEntityToDrop.getCatalogId(), refreshEntityToDrop.getId())) {
        return new DropEntityResult(BaseResult.ReturnStatus.NAMESPACE_NOT_EMPTY, null);
      }
    }

    // simply delete that entity. Will be removed from entities_active, added to the
    // entities_dropped and its version will be changed.
    this.dropEntity(ms, refreshEntityToDrop);

    // if cleanup, schedule a cleanup task for the entity. do this here, so that drop and scheduling
    // the cleanup task is transactional. Otherwise, we'll be unable to schedule the cleanup task
    // later
    if (cleanup) {
      PolarisBaseEntity taskEntity =
          new PolarisEntity.Builder()
              .setId(generateNewEntityId(ms).getId())
              .setCatalogId(0L)
              .setName("entityCleanup_" + entityToDrop.getId())
              .setType(PolarisEntityType.TASK)
              .setSubType(PolarisEntitySubType.NULL_SUBTYPE)
              .setCreateTimestamp(clock.millis())
              .build();

      Map<String, String> properties = new HashMap<>();
      properties.put(
          PolarisTaskConstants.TASK_TYPE,
          String.valueOf(AsyncTaskType.ENTITY_CLEANUP_SCHEDULER.typeCode()));
      properties.put("data", PolarisObjectMapperUtil.serialize(diagnostics, refreshEntityToDrop));
      taskEntity.setProperties(
          PolarisObjectMapperUtil.serializeProperties(diagnostics, properties));
      if (cleanupProperties != null) {
        taskEntity.setInternalProperties(
            PolarisObjectMapperUtil.serializeProperties(diagnostics, cleanupProperties));
      }
      doCreateEntityIfNotExists(ms, null, taskEntity);
      return new DropEntityResult(taskEntity.getId());
    }

    // done, return success
    return new DropEntityResult();
  }

  /** {@inheritDoc} */
  @Override
  public @Nonnull DropEntityResult dropEntityIfExists(
      PolarisMetaStoreSession ms,
      @Nullable List<PolarisEntityCore> catalogPath,
      @Nonnull PolarisEntityCore entityToDrop,
      @Nullable Map<String, String> cleanupProperties,
      boolean cleanup) {
    // get metastore we should be using

    // need to run inside a read/write transaction
    return ms.runInTransaction(
        () -> this.doDropEntityIfExists(ms, catalogPath, entityToDrop, cleanupProperties, cleanup));
  }

  /**
   * Resolve the arguments of granting/revoking a usage grant between a role (catalog or principal
   * role) and a grantee (either a principal role or a principal)
   *
   * @param ms meta store in read/write mode
   * @param catalog if the role is a catalog role, the caller needs to pass-in the catalog entity
   *     which was used to resolve that role. Else null.
   * @param role the role, either a catalog or principal role
   * @param grantee the grantee
   * @return resolver for the specified entities
   */
  private @Nonnull PolarisEntityResolver resolveRoleToGranteeUsageGrant(
      @Nonnull PolarisMetaStoreSession ms,
      @Nullable PolarisEntityCore catalog,
      @Nonnull PolarisEntityCore role,
      @Nonnull PolarisEntityCore grantee) {

    // validate the grantee input
    diagnostics.checkNotNull(grantee, "unexpected_null_grantee");
    diagnostics.check(grantee.getType().isGrantee(), "not_a_grantee", "grantee={}", grantee);

    // validate role
    diagnostics.checkNotNull(role, "unexpected_null_role");

    // role should be a catalog or a principal role
    boolean isCatalogRole = role.getTypeCode() == PolarisEntityType.CATALOG_ROLE.getCode();
    boolean isPrincipalRole = role.getTypeCode() == PolarisEntityType.PRINCIPAL_ROLE.getCode();
    diagnostics.check(isCatalogRole || isPrincipalRole, "not_a_role");

    // if the role is a catalog role, ensure a catalog is specified and
    // vice-versa, catalog should be null if the role is a principal role
    diagnostics.check(
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
        diagnostics, ms, catalog != null ? List.of(catalog) : null, null, otherTopLevelEntities);
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
      @Nonnull PolarisMetaStoreSession ms,
      @Nonnull PolarisEntityCore grantee,
      @Nullable List<PolarisEntityCore> catalogPath,
      @Nonnull PolarisEntityCore securable) {
    // validate role input
    diagnostics.checkNotNull(grantee, "unexpected_null_grantee");
    diagnostics.check(grantee.getType().isGrantee(), "not_grantee_type", "grantee={}", grantee);

    // securable must be supplied
    diagnostics.checkNotNull(securable, "unexpected_null_securable");
    if (securable.getCatalogId() > 0) {
      // catalogPath must be supplied if the securable has a catalogId
      diagnostics.checkNotNull(catalogPath, "unexpected_null_catalogPath");
    }

    // re-resolve now all these entities
    return new PolarisEntityResolver(diagnostics, ms, catalogPath, securable, List.of(grantee));
  }

  /**
   * See {@link #grantUsageOnRoleToGrantee(PolarisMetaStoreSession, PolarisEntityCore,
   * PolarisEntityCore, PolarisEntityCore)}
   */
  private @Nonnull PrivilegeResult doGrantUsageOnRoleToGrantee(
      @Nonnull PolarisMetaStoreSession ms,
      @Nullable PolarisEntityCore catalog,
      @Nonnull PolarisEntityCore role,
      @Nonnull PolarisEntityCore grantee) {

    // ensure these entities have not changed
    PolarisEntityResolver resolver =
        this.resolveRoleToGranteeUsageGrant(ms, catalog, role, grantee);

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
    diagnostics.check(grantee.getType().isGrantee(), "not_a_grantee", "grantee={}", grantee);
    PolarisGrantRecord grantRecord = this.persistNewGrantRecord(ms, role, grantee, usagePriv);
    return new PrivilegeResult(grantRecord);
  }

  /** {@inheritDoc} */
  @Override
  public @Nonnull PrivilegeResult grantUsageOnRoleToGrantee(
      PolarisMetaStoreSession ms,
      @Nullable PolarisEntityCore catalog,
      @Nonnull PolarisEntityCore role,
      @Nonnull PolarisEntityCore grantee) {
    return ms.runInTransaction(() -> this.doGrantUsageOnRoleToGrantee(ms, catalog, role, grantee));
  }

  /**
   * See {@link #revokeUsageOnRoleFromGrantee(PolarisMetaStoreSession, PolarisEntityCore,
   * PolarisEntityCore, PolarisEntityCore)}
   */
  private @Nonnull PrivilegeResult doRevokeUsageOnRoleFromGrantee(
      @Nonnull PolarisMetaStoreSession ms,
      @Nullable PolarisEntityCore catalog,
      @Nonnull PolarisEntityCore role,
      @Nonnull PolarisEntityCore grantee) {

    // ensure these entities have not changed
    PolarisEntityResolver resolver =
        this.resolveRoleToGranteeUsageGrant(ms, catalog, role, grantee);

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
    this.revokeGrantRecord(ms, role, grantee, grantRecord);

    return new PrivilegeResult(grantRecord);
  }

  /** {@inheritDoc} */
  @Override
  public @Nonnull PrivilegeResult revokeUsageOnRoleFromGrantee(
      PolarisMetaStoreSession ms,
      @Nullable PolarisEntityCore catalog,
      @Nonnull PolarisEntityCore role,
      @Nonnull PolarisEntityCore grantee) {
    return ms.runInTransaction(
        () -> this.doRevokeUsageOnRoleFromGrantee(ms, catalog, role, grantee));
  }

  /**
   * See {@link #grantPrivilegeOnSecurableToRole(PolarisMetaStoreSession, PolarisEntityCore, List,
   * PolarisEntityCore, PolarisPrivilege)}
   */
  private @Nonnull PrivilegeResult doGrantPrivilegeOnSecurableToRole(
      @Nonnull PolarisMetaStoreSession ms,
      @Nonnull PolarisEntityCore grantee,
      @Nullable List<PolarisEntityCore> catalogPath,
      @Nonnull PolarisEntityCore securable,
      @Nonnull PolarisPrivilege priv) {

    // re-resolve now all these entities
    PolarisEntityResolver resolver =
        this.resolveSecurableToRoleGrant(ms, grantee, catalogPath, securable);

    // if failure to resolve, let the caller know
    if (resolver.isFailure()) {
      return new PrivilegeResult(BaseResult.ReturnStatus.ENTITY_CANNOT_BE_RESOLVED, null);
    }

    // grant specified privilege on this securable to this role and return the grant
    PolarisGrantRecord grantRecord = this.persistNewGrantRecord(ms, securable, grantee, priv);
    return new PrivilegeResult(grantRecord);
  }

  /** {@inheritDoc} */
  @Override
  public @Nonnull PrivilegeResult grantPrivilegeOnSecurableToRole(
      PolarisMetaStoreSession ms,
      @Nonnull PolarisEntityCore grantee,
      @Nullable List<PolarisEntityCore> catalogPath,
      @Nonnull PolarisEntityCore securable,
      @Nonnull PolarisPrivilege privilege) {
    return ms.runInTransaction(
        () ->
            this.doGrantPrivilegeOnSecurableToRole(ms, grantee, catalogPath, securable, privilege));
  }

  /**
   * See {@link #revokePrivilegeOnSecurableFromRole(PolarisMetaStoreSession, PolarisEntityCore,
   * List, PolarisEntityCore, PolarisPrivilege)}
   */
  private @Nonnull PrivilegeResult doRevokePrivilegeOnSecurableFromRole(
      @Nonnull PolarisMetaStoreSession ms,
      @Nonnull PolarisEntityCore grantee,
      @Nullable List<PolarisEntityCore> catalogPath,
      @Nonnull PolarisEntityCore securable,
      @Nonnull PolarisPrivilege priv) {

    // re-resolve now all these entities
    PolarisEntityResolver resolver =
        this.resolveSecurableToRoleGrant(ms, grantee, catalogPath, securable);

    // if failure to resolve, let the caller know
    if (resolver.isFailure()) {
      return new PrivilegeResult(BaseResult.ReturnStatus.ENTITY_CANNOT_BE_RESOLVED, null);
    }

    // lookup the grants records to find this grant
    PolarisGrantRecord grantRecord =
        ms.lookupGrantRecord(
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
    this.revokeGrantRecord(ms, securable, grantee, grantRecord);

    // success!
    return new PrivilegeResult(grantRecord);
  }

  /** {@inheritDoc} */
  @Override
  public @Nonnull PrivilegeResult revokePrivilegeOnSecurableFromRole(
      PolarisMetaStoreSession ms,
      @Nonnull PolarisEntityCore grantee,
      @Nullable List<PolarisEntityCore> catalogPath,
      @Nonnull PolarisEntityCore securable,
      @Nonnull PolarisPrivilege privilege) {
    return ms.runInTransaction(
        () ->
            this.doRevokePrivilegeOnSecurableFromRole(
                ms, grantee, catalogPath, securable, privilege));
  }

  /** {@link #loadGrantsOnSecurable(PolarisMetaStoreSession, long, long)} */
  private @Nonnull LoadGrantsResult doLoadGrantsOnSecurable(
      @Nonnull PolarisMetaStoreSession ms, long securableCatalogId, long securableId) {

    // lookup grants version for this securable entity
    int grantsVersion = ms.lookupEntityGrantRecordsVersion(securableCatalogId, securableId);

    // return null if securable does not exists
    if (grantsVersion == 0) {
      return new LoadGrantsResult(BaseResult.ReturnStatus.ENTITY_NOT_FOUND, null);
    }

    // now fetch all grants for this securable
    final List<PolarisGrantRecord> returnGrantRecords =
        ms.loadAllGrantRecordsOnSecurable(securableCatalogId, securableId);

    // find all unique grantees
    List<PolarisEntityId> entityIds =
        returnGrantRecords.stream()
            .map(
                grantRecord ->
                    new PolarisEntityId(
                        grantRecord.getGranteeCatalogId(), grantRecord.getGranteeId()))
            .distinct()
            .collect(Collectors.toList());
    List<PolarisBaseEntity> entities = ms.lookupEntities(entityIds);

    // done, return the list of grants and their version
    return new LoadGrantsResult(
        grantsVersion,
        returnGrantRecords,
        entities.stream().filter(Objects::nonNull).collect(Collectors.toList()));
  }

  /** {@inheritDoc} */
  @Override
  public @Nonnull LoadGrantsResult loadGrantsOnSecurable(
      PolarisMetaStoreSession ms, long securableCatalogId, long securableId) {
    return ms.runInReadTransaction(
        () -> this.doLoadGrantsOnSecurable(ms, securableCatalogId, securableId));
  }

  /** {@link #loadGrantsToGrantee(PolarisMetaStoreSession, long, long)} */
  private @Nonnull LoadGrantsResult doLoadGrantsToGrantee(
      @Nonnull PolarisMetaStoreSession ms, long granteeCatalogId, long granteeId) {

    // lookup grants version for this grantee entity
    int grantsVersion = ms.lookupEntityGrantRecordsVersion(granteeCatalogId, granteeId);

    // return null if grantee does not exists
    if (grantsVersion == 0) {
      return new LoadGrantsResult(BaseResult.ReturnStatus.ENTITY_NOT_FOUND, null);
    }

    // now fetch all grants for this grantee
    final List<PolarisGrantRecord> returnGrantRecords =
        ms.loadAllGrantRecordsOnGrantee(granteeCatalogId, granteeId);

    // find all unique securables
    List<PolarisEntityId> entityIds =
        returnGrantRecords.stream()
            .map(
                grantRecord ->
                    new PolarisEntityId(
                        grantRecord.getSecurableCatalogId(), grantRecord.getSecurableId()))
            .distinct()
            .collect(Collectors.toList());
    List<PolarisBaseEntity> entities = ms.lookupEntities(entityIds);

    // done, return the list of grants and their version
    return new LoadGrantsResult(
        grantsVersion,
        returnGrantRecords,
        entities.stream().filter(Objects::nonNull).collect(Collectors.toList()));
  }

  /** {@inheritDoc} */
  @Override
  public @Nonnull LoadGrantsResult loadGrantsToGrantee(
      PolarisMetaStoreSession ms, long granteeCatalogId, long granteeId) {
    return ms.runInReadTransaction(
        () -> this.doLoadGrantsToGrantee(ms, granteeCatalogId, granteeId));
  }

  /** {@link PolarisMetaStoreManager#loadEntitiesChangeTracking(PolarisMetaStoreSession, List)} */
  private @Nonnull ChangeTrackingResult doLoadEntitiesChangeTracking(
      @Nonnull PolarisMetaStoreSession ms, @Nonnull List<PolarisEntityId> entityIds) {
    List<PolarisChangeTrackingVersions> changeTracking = ms.lookupEntityVersions(entityIds);
    return new ChangeTrackingResult(changeTracking);
  }

  /** {@inheritDoc} */
  @Override
  public @Nonnull ChangeTrackingResult loadEntitiesChangeTracking(
      PolarisMetaStoreSession ms, @Nonnull List<PolarisEntityId> entityIds) {
    return ms.runInReadTransaction(() -> this.doLoadEntitiesChangeTracking(ms, entityIds));
  }

  /** Refer to {@link #loadEntity(PolarisMetaStoreSession, long, long)} */
  private @Nonnull EntityResult doLoadEntity(
      @Nonnull PolarisMetaStoreSession ms, long entityCatalogId, long entityId) {
    // this is an easy one
    PolarisBaseEntity entity = ms.lookupEntity(entityCatalogId, entityId);
    return (entity != null)
        ? new EntityResult(entity)
        : new EntityResult(BaseResult.ReturnStatus.ENTITY_NOT_FOUND, null);
  }

  /** {@inheritDoc} */
  @Override
  public @Nonnull EntityResult loadEntity(
      PolarisMetaStoreSession ms, long entityCatalogId, long entityId) {
    return ms.runInReadTransaction(() -> this.doLoadEntity(ms, entityCatalogId, entityId));
  }

  /** Refer to {@link #loadTasks(PolarisMetaStoreSession, String, int)} */
  private @Nonnull EntitiesResult doLoadTasks(
      @Nonnull PolarisMetaStoreSession ms, String executorId, int limit) {

    // find all available tasks
    List<PolarisBaseEntity> availableTasks =
        ms.listActiveEntities(
            PolarisEntityConstants.getRootEntityId(),
            PolarisEntityConstants.getRootEntityId(),
            PolarisEntityType.TASK,
            limit,
            entity -> {
              PolarisObjectMapperUtil.TaskExecutionState taskState =
                  PolarisObjectMapperUtil.parseTaskState(entity);
              long taskAgeTimeout =
                  configurationStore.getConfiguration(
                      realm,
                      PolarisTaskConstants.TASK_TIMEOUT_MILLIS_CONFIG,
                      PolarisTaskConstants.TASK_TIMEOUT_MILLIS);
              return taskState == null
                  || taskState.executor == null
                  || clock.millis() - taskState.lastAttemptStartTime > taskAgeTimeout;
            },
            Function.identity());

    availableTasks.forEach(
        task -> {
          Map<String, String> properties =
              PolarisObjectMapperUtil.deserializeProperties(diagnostics, task.getProperties());
          properties.put(PolarisTaskConstants.LAST_ATTEMPT_EXECUTOR_ID, executorId);
          properties.put(
              PolarisTaskConstants.LAST_ATTEMPT_START_TIME, String.valueOf(clock.millis()));
          properties.put(
              PolarisTaskConstants.ATTEMPT_COUNT,
              String.valueOf(
                  Integer.parseInt(properties.getOrDefault(PolarisTaskConstants.ATTEMPT_COUNT, "0"))
                      + 1));
          task.setEntityVersion(task.getEntityVersion() + 1);
          task.setProperties(PolarisObjectMapperUtil.serializeProperties(diagnostics, properties));
          writeEntity(ms, task, false);
        });
    return new EntitiesResult(availableTasks);
  }

  @Override
  public @Nonnull EntitiesResult loadTasks(
      PolarisMetaStoreSession ms, String executorId, int limit) {
    return ms.runInTransaction(() -> this.doLoadTasks(ms, executorId, limit));
  }

  /** {@inheritDoc} */
  @Override
  public @Nonnull ScopedCredentialsResult getSubscopedCredsForEntity(
      @Nonnull PolarisMetaStoreSession ms,
      long catalogId,
      long entityId,
      boolean allowListOperation,
      @Nonnull Set<String> allowedReadLocations,
      @Nonnull Set<String> allowedWriteLocations) {

    // get meta store session we should be using
    diagnostics.check(
        !allowedReadLocations.isEmpty() || !allowedWriteLocations.isEmpty(),
        "allowed_locations_to_subscope_is_required");

    // reload the entity, error out if not found
    EntityResult reloadedEntity = loadEntity(ms, catalogId, entityId);
    if (reloadedEntity.getReturnStatus() != BaseResult.ReturnStatus.SUCCESS) {
      return new ScopedCredentialsResult(
          reloadedEntity.getReturnStatus(), reloadedEntity.getExtraInformation());
    }

    // get storage integration
    PolarisStorageIntegration<PolarisStorageConfigurationInfo> storageIntegration =
        ms.loadPolarisStorageIntegration(reloadedEntity.getEntity());

    // cannot be null
    diagnostics.checkNotNull(
        storageIntegration,
        "storage_integration_not_exists",
        "catalogId={}, entityId={}",
        catalogId,
        entityId);

    PolarisStorageConfigurationInfo storageConfigurationInfo =
        readStorageConfiguration(diagnostics, reloadedEntity.getEntity());
    try {
      EnumMap<PolarisCredentialProperty, String> creds =
          storageIntegration.getSubscopedCreds(
              realm,
              diagnostics,
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
  public @Nonnull ValidateAccessResult validateAccessToLocations(
      @Nonnull PolarisMetaStoreSession metaStoreSession,
      long catalogId,
      long entityId,
      @Nonnull Set<PolarisStorageActions> actions,
      @Nonnull Set<String> locations) {
    // get meta store we should be using
    diagnostics.check(
        !actions.isEmpty() && !locations.isEmpty(),
        "locations_and_operations_privileges_are_required");
    // reload the entity, error out if not found
    EntityResult reloadedEntity = loadEntity(metaStoreSession, catalogId, entityId);
    if (reloadedEntity.getReturnStatus() != BaseResult.ReturnStatus.SUCCESS) {
      return new ValidateAccessResult(
          reloadedEntity.getReturnStatus(), reloadedEntity.getExtraInformation());
    }

    // get storage integration, expect not null
    PolarisStorageIntegration<PolarisStorageConfigurationInfo> storageIntegration =
        metaStoreSession.loadPolarisStorageIntegration(reloadedEntity.getEntity());
    diagnostics.checkNotNull(
        storageIntegration,
        "storage_integration_not_exists",
        "catalogId={}, entityId={}",
        catalogId,
        entityId);

    // validate access
    PolarisStorageConfigurationInfo storageConfigurationInfo =
        readStorageConfiguration(diagnostics, reloadedEntity.getEntity());
    Map<String, String> validateLocationAccess =
        storageIntegration
            .validateAccessToLocations(realm, storageConfigurationInfo, actions, locations)
            .entrySet()
            .stream()
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey,
                    e -> PolarisObjectMapperUtil.serialize(diagnostics, e.getValue())));

    // done, return result
    return new ValidateAccessResult(validateLocationAccess);
  }

  public static PolarisStorageConfigurationInfo readStorageConfiguration(
      PolarisDiagnostics diagnostics, PolarisBaseEntity reloadedEntity) {
    Map<String, String> propMap =
        PolarisObjectMapperUtil.deserializeProperties(
            diagnostics, reloadedEntity.getInternalProperties());
    String storageConfigInfoStr =
        propMap.get(PolarisEntityConstants.getStorageConfigInfoPropertyName());

    diagnostics.check(
        storageConfigInfoStr != null,
        "missing_storage_configuration_info",
        "catalogId={}, entityId={}",
        reloadedEntity.getCatalogId(),
        reloadedEntity.getId());
    return PolarisStorageConfigurationInfo.deserialize(diagnostics, storageConfigInfoStr);
  }

  /**
   * Get the internal property map for an entity
   *
   * @param entity the target entity
   * @return a map of string representing the internal properties
   */
  public Map<String, String> getInternalPropertyMap(@Nonnull PolarisBaseEntity entity) {
    String internalPropStr = entity.getInternalProperties();
    Map<String, String> res = new HashMap<>();
    if (internalPropStr == null) {
      return res;
    }
    return deserializeProperties(internalPropStr);
  }

  /** {@link #loadCachedEntryById(PolarisMetaStoreSession, long, long)} */
  private @Nonnull CachedEntryResult doLoadCachedEntryById(
      @Nonnull PolarisMetaStoreSession ms, long entityCatalogId, long entityId) {

    // load that entity
    PolarisBaseEntity entity = ms.lookupEntity(entityCatalogId, entityId);

    // if entity not found, return null
    if (entity == null) {
      return new CachedEntryResult(BaseResult.ReturnStatus.ENTITY_NOT_FOUND, null);
    }

    // load the grant records
    final List<PolarisGrantRecord> grantRecords;
    if (entity.getType().isGrantee()) {
      grantRecords = new ArrayList<>(ms.loadAllGrantRecordsOnGrantee(entityCatalogId, entityId));
      grantRecords.addAll(ms.loadAllGrantRecordsOnSecurable(entityCatalogId, entityId));
    } else {
      grantRecords = ms.loadAllGrantRecordsOnSecurable(entityCatalogId, entityId);
    }

    // return the result
    return new CachedEntryResult(entity, entity.getGrantRecordsVersion(), grantRecords);
  }

  /** {@inheritDoc} */
  @Override
  public @Nonnull CachedEntryResult loadCachedEntryById(
      PolarisMetaStoreSession ms, long entityCatalogId, long entityId) {
    return ms.runInReadTransaction(() -> this.doLoadCachedEntryById(ms, entityCatalogId, entityId));
  }

  /**
   * {@link #loadCachedEntryByName(PolarisMetaStoreSession, long, long, PolarisEntityType, String)}
   */
  private @Nonnull CachedEntryResult doLoadCachedEntryByName(
      @Nonnull PolarisMetaStoreSession ms,
      long entityCatalogId,
      long parentId,
      @Nonnull PolarisEntityType entityType,
      @Nonnull String entityName) {

    // load that entity
    PolarisEntitiesActiveKey entityActiveKey =
        new PolarisEntitiesActiveKey(entityCatalogId, parentId, entityType.getCode(), entityName);
    PolarisBaseEntity entity = this.lookupEntityByName(ms, entityActiveKey);

    // null if entity not found
    if (entity == null) {
      return new CachedEntryResult(BaseResult.ReturnStatus.ENTITY_NOT_FOUND, null);
    }

    // load the grant records
    final List<PolarisGrantRecord> grantRecords;
    if (entity.getType().isGrantee()) {
      grantRecords =
          new ArrayList<>(ms.loadAllGrantRecordsOnGrantee(entityCatalogId, entity.getId()));
      grantRecords.addAll(ms.loadAllGrantRecordsOnSecurable(entityCatalogId, entity.getId()));
    } else {
      grantRecords = ms.loadAllGrantRecordsOnSecurable(entityCatalogId, entity.getId());
    }

    // return the result
    return new CachedEntryResult(entity, entity.getGrantRecordsVersion(), grantRecords);
  }

  /** {@inheritDoc} */
  @Override
  public @Nonnull CachedEntryResult loadCachedEntryByName(
      PolarisMetaStoreSession ms,
      long entityCatalogId,
      long parentId,
      @Nonnull PolarisEntityType entityType,
      @Nonnull String entityName) {
    CachedEntryResult result =
        ms.runInReadTransaction(
            () ->
                this.doLoadCachedEntryByName(
                    ms, entityCatalogId, parentId, entityType, entityName));
    if (PolarisEntityConstants.getRootContainerName().equals(entityName)
        && entityType == PolarisEntityType.ROOT
        && !result.isSuccess()) {
      // Backfill rootContainer if needed.
      ms.runActionInTransaction(
          () -> {
            PolarisBaseEntity rootContainer =
                new PolarisBaseEntity(
                    PolarisEntityConstants.getNullId(),
                    PolarisEntityConstants.getRootEntityId(),
                    PolarisEntityType.ROOT,
                    PolarisEntitySubType.NULL_SUBTYPE,
                    PolarisEntityConstants.getRootEntityId(),
                    PolarisEntityConstants.getRootContainerName());
            EntityResult backfillResult = this.createEntityIfNotExists(ms, null, rootContainer);
            if (backfillResult.isSuccess()) {
              PolarisEntitiesActiveKey serviceAdminRoleKey =
                  new PolarisEntitiesActiveKey(
                      0L,
                      0L,
                      PolarisEntityType.PRINCIPAL_ROLE.getCode(),
                      PolarisEntityConstants.getNameOfPrincipalServiceAdminRole());
              PolarisBaseEntity serviceAdminRole = this.lookupEntityByName(ms, serviceAdminRoleKey);
              if (serviceAdminRole != null) {
                this.persistNewGrantRecord(
                    ms, rootContainer, serviceAdminRole, PolarisPrivilege.SERVICE_MANAGE_ACCESS);
              }
            }
          });

      // Redo the lookup in a separate read transaction.
      result =
          ms.runInReadTransaction(
              () ->
                  this.doLoadCachedEntryByName(
                      ms, entityCatalogId, parentId, entityType, entityName));
    }
    return result;
  }

  /** {@inheritDoc} */
  private @Nonnull CachedEntryResult doRefreshCachedEntity(
      @Nonnull PolarisMetaStoreSession ms,
      int entityVersion,
      int entityGrantRecordsVersion,
      @Nonnull PolarisEntityType entityType,
      long entityCatalogId,
      long entityId) {

    // load version information
    PolarisChangeTrackingVersions entityVersions =
        ms.lookupEntityVersions(List.of(new PolarisEntityId(entityCatalogId, entityId))).get(0);

    // if null, the entity has been purged
    if (entityVersions == null) {
      return new CachedEntryResult(BaseResult.ReturnStatus.ENTITY_NOT_FOUND, null);
    }

    // load the entity if something changed
    final PolarisBaseEntity entity;
    if (entityVersion != entityVersions.getEntityVersion()) {
      entity = ms.lookupEntity(entityCatalogId, entityId);

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
        grantRecords = new ArrayList<>(ms.loadAllGrantRecordsOnGrantee(entityCatalogId, entityId));
        grantRecords.addAll(ms.loadAllGrantRecordsOnSecurable(entityCatalogId, entityId));
      } else {
        grantRecords = ms.loadAllGrantRecordsOnSecurable(entityCatalogId, entityId);
      }
    } else {
      grantRecords = null;
    }

    // return the result
    return new CachedEntryResult(entity, entityVersions.getGrantRecordsVersion(), grantRecords);
  }

  /** {@inheritDoc} */
  @Override
  public @Nonnull CachedEntryResult refreshCachedEntity(
      PolarisMetaStoreSession ms,
      int entityVersion,
      int entityGrantRecordsVersion,
      @Nonnull PolarisEntityType entityType,
      long entityCatalogId,
      long entityId) {
    return ms.runInReadTransaction(
        () ->
            this.doRefreshCachedEntity(
                ms,
                entityVersion,
                entityGrantRecordsVersion,
                entityType,
                entityCatalogId,
                entityId));
  }
}
