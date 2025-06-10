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
package org.apache.polaris.extension.persistence.impl.eclipselink;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import jakarta.persistence.EntityManager;
import jakarta.persistence.TypedQuery;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.entity.EntityNameLookupRecord;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntitiesActiveKey;
import org.apache.polaris.core.entity.PolarisEntityCore;
import org.apache.polaris.core.entity.PolarisEntityId;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.PolarisGrantRecord;
import org.apache.polaris.core.entity.PolarisPrincipalSecrets;
import org.apache.polaris.core.persistence.pagination.EntityIdToken;
import org.apache.polaris.core.persistence.pagination.PageToken;
import org.apache.polaris.core.policy.PolarisPolicyMappingRecord;
import org.apache.polaris.core.policy.PolicyEntity;
import org.apache.polaris.extension.persistence.impl.eclipselink.models.ModelEntity;
import org.apache.polaris.extension.persistence.impl.eclipselink.models.ModelEntityActive;
import org.apache.polaris.extension.persistence.impl.eclipselink.models.ModelEntityChangeTracking;
import org.apache.polaris.extension.persistence.impl.eclipselink.models.ModelGrantRecord;
import org.apache.polaris.extension.persistence.impl.eclipselink.models.ModelPolicyMappingRecord;
import org.apache.polaris.extension.persistence.impl.eclipselink.models.ModelPrincipalSecrets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implements an EclipseLink based metastore for Polaris which can be configured for any database
 * with EclipseLink support
 */
public class PolarisEclipseLinkStore {
  private static final Logger LOGGER = LoggerFactory.getLogger(PolarisEclipseLinkStore.class);

  // diagnostic services
  private final PolarisDiagnostics diagnosticServices;

  // Used to track when the store is initialized
  private final AtomicBoolean initialized = new AtomicBoolean(false);

  /**
   * Constructor, allocate everything at once
   *
   * @param diagnostics diagnostic services
   */
  public PolarisEclipseLinkStore(@Nonnull PolarisDiagnostics diagnostics) {
    this.diagnosticServices = diagnostics;
  }

  /** Initialize the store. This should be called before other methods. */
  public void initialize(EntityManager session) {
    PolarisSequenceUtil.initialize(session);
    initialized.set(true);
  }

  long getNextSequence(EntityManager session) {
    diagnosticServices.check(session != null, "session_is_null");
    checkInitialized();

    return PolarisSequenceUtil.getNewId(session);
  }

  void writeToEntities(EntityManager session, PolarisBaseEntity entity) {
    diagnosticServices.check(session != null, "session_is_null");
    checkInitialized();

    ModelEntity model =
        lookupEntity(session, entity.getCatalogId(), entity.getId(), entity.getTypeCode());
    if (model != null) {
      // Update if the same entity already exists
      model.update(entity);
    } else {
      model = ModelEntity.fromEntity(entity);
    }

    session.persist(model);
  }

  void writeToEntitiesActive(EntityManager session, PolarisBaseEntity entity) {
    diagnosticServices.check(session != null, "session_is_null");
    checkInitialized();

    ModelEntityActive model = lookupEntityActive(session, new PolarisEntitiesActiveKey(entity));
    if (model == null) {
      session.persist(ModelEntityActive.fromEntityActive(new EntityNameLookupRecord(entity)));
    }
  }

  void writeToEntitiesChangeTracking(EntityManager session, PolarisBaseEntity entity) {
    diagnosticServices.check(session != null, "session_is_null");
    checkInitialized();

    // Update the existing change tracking if a record with the same ids exists; otherwise, persist
    // a new one
    ModelEntityChangeTracking entityChangeTracking =
        lookupEntityChangeTracking(session, entity.getCatalogId(), entity.getId());
    if (entityChangeTracking != null) {
      entityChangeTracking.update(entity);
    } else {
      entityChangeTracking = new ModelEntityChangeTracking(entity);
    }

    session.persist(entityChangeTracking);
  }

  void writeToGrantRecords(EntityManager session, PolarisGrantRecord grantRec) {
    diagnosticServices.check(session != null, "session_is_null");
    checkInitialized();

    session.persist(ModelGrantRecord.fromGrantRecord(grantRec));
  }

  void deleteFromEntities(EntityManager session, long catalogId, long entityId, int typeCode) {
    diagnosticServices.check(session != null, "session_is_null");
    checkInitialized();

    ModelEntity model = lookupEntity(session, catalogId, entityId, typeCode);
    diagnosticServices.check(model != null, "entity_not_found");

    session.remove(model);
  }

  void deleteFromEntitiesActive(EntityManager session, PolarisEntitiesActiveKey key) {
    diagnosticServices.check(session != null, "session_is_null");
    checkInitialized();

    ModelEntityActive entity = lookupEntityActive(session, key);
    diagnosticServices.check(entity != null, "active_entity_not_found");
    session.remove(entity);
  }

  void deleteFromEntitiesChangeTracking(EntityManager session, PolarisEntityCore entity) {
    diagnosticServices.check(session != null, "session_is_null");
    checkInitialized();

    ModelEntityChangeTracking entityChangeTracking =
        lookupEntityChangeTracking(session, entity.getCatalogId(), entity.getId());
    diagnosticServices.check(entityChangeTracking != null, "change_tracking_entity_not_found");

    session.remove(entityChangeTracking);
  }

  void deleteFromGrantRecords(EntityManager session, PolarisGrantRecord grantRec) {
    diagnosticServices.check(session != null, "session_is_null");
    checkInitialized();

    ModelGrantRecord lookupGrantRecord =
        lookupGrantRecord(
            session,
            grantRec.getSecurableCatalogId(),
            grantRec.getSecurableId(),
            grantRec.getGranteeCatalogId(),
            grantRec.getGranteeId(),
            grantRec.getPrivilegeCode());

    diagnosticServices.check(lookupGrantRecord != null, "grant_record_not_found");

    session.remove(lookupGrantRecord);
  }

  void deleteAllEntityGrantRecords(EntityManager session, PolarisEntityCore entity) {
    diagnosticServices.check(session != null, "session_is_null");
    checkInitialized();

    // Delete grant records from grantRecords tables
    lookupAllGrantRecordsOnSecurable(session, entity.getCatalogId(), entity.getId())
        .forEach(session::remove);

    // Delete grantee records from grantRecords tables
    lookupGrantRecordsOnGrantee(session, entity.getCatalogId(), entity.getId())
        .forEach(session::remove);
  }

  void deleteAll(EntityManager session) {
    diagnosticServices.check(session != null, "session_is_null");
    checkInitialized();

    session.createQuery("DELETE from ModelEntity").executeUpdate();
    session.createQuery("DELETE from ModelEntityActive").executeUpdate();
    session.createQuery("DELETE from ModelEntityChangeTracking").executeUpdate();
    session.createQuery("DELETE from ModelGrantRecord").executeUpdate();
    session.createQuery("DELETE from ModelPrincipalSecrets").executeUpdate();

    LOGGER.debug("All entities deleted.");
  }

  ModelEntity lookupEntity(EntityManager session, long catalogId, long entityId, long typeCode) {
    diagnosticServices.check(session != null, "session_is_null");
    checkInitialized();

    return session
        .createQuery(
            "SELECT m from ModelEntity m where m.catalogId=:catalogId and m.id=:id and m.typeCode=:typeCode",
            ModelEntity.class)
        .setParameter("typeCode", typeCode)
        .setParameter("catalogId", catalogId)
        .setParameter("id", entityId)
        .getResultStream()
        .findFirst()
        .orElse(null);
  }

  @SuppressWarnings("unchecked")
  List<ModelEntity> lookupEntities(EntityManager session, List<PolarisEntityId> entityIds) {
    diagnosticServices.check(session != null, "session_is_null");
    checkInitialized();

    if (entityIds == null || entityIds.isEmpty()) return new ArrayList<>();

    // TODO Support paging
    String inClause =
        entityIds.stream()
            .map(entityId -> "(" + entityId.getCatalogId() + "," + entityId.getId() + ")")
            .collect(Collectors.joining(","));

    String hql = "SELECT * from ENTITIES m where (m.catalogId, m.id) in (" + inClause + ")";
    return (List<ModelEntity>) session.createNativeQuery(hql, ModelEntity.class).getResultList();
  }

  ModelEntityActive lookupEntityActive(
      EntityManager session, PolarisEntitiesActiveKey entityActiveKey) {
    diagnosticServices.check(session != null, "session_is_null");
    checkInitialized();

    return session
        .createQuery(
            "SELECT m from ModelEntityActive m where m.catalogId=:catalogId and m.parentId=:parentId and m.typeCode=:typeCode and m.name=:name",
            ModelEntityActive.class)
        .setParameter("catalogId", entityActiveKey.getCatalogId())
        .setParameter("parentId", entityActiveKey.getParentId())
        .setParameter("typeCode", entityActiveKey.getTypeCode())
        .setParameter("name", entityActiveKey.getName())
        .getResultStream()
        .findFirst()
        .orElse(null);
  }

  long countActiveChildEntities(
      EntityManager session,
      long catalogId,
      long parentId,
      @Nullable PolarisEntityType entityType) {
    diagnosticServices.check(session != null, "session_is_null");
    checkInitialized();

    String hql =
        "SELECT COUNT(m) from ModelEntityActive m where m.catalogId=:catalogId and m.parentId=:parentId";
    if (entityType != null) {
      hql += " and m.typeCode=:typeCode";
    }

    TypedQuery<Long> query =
        session
            .createQuery(hql, Long.class)
            .setParameter("catalogId", catalogId)
            .setParameter("parentId", parentId);
    if (entityType != null) {
      query.setParameter("typeCode", entityType.getCode());
    }

    return query.getSingleResult();
  }

  List<ModelEntity> lookupFullEntitiesActive(
      EntityManager session,
      long catalogId,
      long parentId,
      @Nonnull PolarisEntityType entityType,
      @Nonnull PageToken pageToken) {
    diagnosticServices.check(session != null, "session_is_null");
    checkInitialized();

    // Currently check against ENTITIES not joining with ENTITIES_ACTIVE
    String hql =
        "SELECT m from ModelEntity m where"
            + " m.catalogId=:catalogId and m.parentId=:parentId and m.typeCode=:typeCode";

    var entityIdToken = pageToken.valueAs(EntityIdToken.class);
    if (entityIdToken.isPresent()) {
      hql += " and m.id > :tokenId";
    }

    if (pageToken.paginationRequested()) {
      hql += " order by m.id asc";
    }

    TypedQuery<ModelEntity> query =
        session
            .createQuery(hql, ModelEntity.class)
            .setParameter("catalogId", catalogId)
            .setParameter("parentId", parentId)
            .setParameter("typeCode", entityType.getCode());

    if (entityIdToken.isPresent()) {
      long tokenId = entityIdToken.get().entityId();
      query = query.setParameter("tokenId", tokenId);
    }

    return query.getResultList();
  }

  ModelEntityChangeTracking lookupEntityChangeTracking(
      EntityManager session, long catalogId, long entityId) {
    diagnosticServices.check(session != null, "session_is_null");
    checkInitialized();

    return session
        .createQuery(
            "SELECT m from ModelEntityChangeTracking m where m.catalogId=:catalogId and m.id=:id",
            ModelEntityChangeTracking.class)
        .setParameter("catalogId", catalogId)
        .setParameter("id", entityId)
        .getResultStream()
        .findFirst()
        .orElse(null);
  }

  ModelGrantRecord lookupGrantRecord(
      EntityManager session,
      long securableCatalogId,
      long securableId,
      long granteeCatalogId,
      long granteeId,
      int privilegeCode) {
    diagnosticServices.check(session != null, "session_is_null");
    checkInitialized();

    return session
        .createQuery(
            "SELECT m from ModelGrantRecord m where m.securableCatalogId=:securableCatalogId "
                + "and m.securableId=:securableId "
                + "and m.granteeCatalogId=:granteeCatalogId "
                + "and m.granteeId=:granteeId "
                + "and m.privilegeCode=:privilegeCode",
            ModelGrantRecord.class)
        .setParameter("securableCatalogId", securableCatalogId)
        .setParameter("securableId", securableId)
        .setParameter("granteeCatalogId", granteeCatalogId)
        .setParameter("granteeId", granteeId)
        .setParameter("privilegeCode", privilegeCode)
        .getResultStream()
        .findFirst()
        .orElse(null);
  }

  List<ModelGrantRecord> lookupAllGrantRecordsOnSecurable(
      EntityManager session, long securableCatalogId, long securableId) {
    diagnosticServices.check(session != null, "session_is_null");
    checkInitialized();

    return session
        .createQuery(
            "SELECT m from ModelGrantRecord m "
                + "where m.securableCatalogId=:securableCatalogId "
                + "and m.securableId=:securableId",
            ModelGrantRecord.class)
        .setParameter("securableCatalogId", securableCatalogId)
        .setParameter("securableId", securableId)
        .getResultList();
  }

  List<ModelGrantRecord> lookupGrantRecordsOnGrantee(
      EntityManager session, long granteeCatalogId, long granteeId) {
    diagnosticServices.check(session != null, "session_is_null");
    checkInitialized();

    return session
        .createQuery(
            "SELECT m from ModelGrantRecord m "
                + "where m.granteeCatalogId=:granteeCatalogId "
                + "and m.granteeId=:granteeId",
            ModelGrantRecord.class)
        .setParameter("granteeCatalogId", granteeCatalogId)
        .setParameter("granteeId", granteeId)
        .getResultList();
  }

  ModelPrincipalSecrets lookupPrincipalSecrets(EntityManager session, String clientId) {
    diagnosticServices.check(session != null, "session_is_null");
    checkInitialized();

    return session
        .createQuery(
            "SELECT m from ModelPrincipalSecrets m where m.principalClientId=:clientId",
            ModelPrincipalSecrets.class)
        .setParameter("clientId", clientId)
        .getResultStream()
        .findFirst()
        .orElse(null);
  }

  void writePrincipalSecrets(EntityManager session, PolarisPrincipalSecrets principalSecrets) {
    diagnosticServices.check(session != null, "session_is_null");
    checkInitialized();

    ModelPrincipalSecrets modelPrincipalSecrets =
        lookupPrincipalSecrets(session, principalSecrets.getPrincipalClientId());
    if (modelPrincipalSecrets != null) {
      modelPrincipalSecrets.update(principalSecrets);
    } else {
      modelPrincipalSecrets = ModelPrincipalSecrets.fromPrincipalSecrets(principalSecrets);
    }

    session.persist(modelPrincipalSecrets);
  }

  void deletePrincipalSecrets(EntityManager session, String clientId) {
    diagnosticServices.check(session != null, "session_is_null");
    checkInitialized();

    ModelPrincipalSecrets modelPrincipalSecrets = lookupPrincipalSecrets(session, clientId);
    diagnosticServices.check(modelPrincipalSecrets != null, "principal_secretes_not_found");

    session.remove(modelPrincipalSecrets);
  }

  void writeToPolicyMappingRecords(
      EntityManager session, PolarisPolicyMappingRecord mappingRecord) {
    diagnosticServices.check(session != null, "session_is_null");
    checkInitialized();

    // TODO: combine existence check and write into one statement
    ModelPolicyMappingRecord model =
        lookupPolicyMappingRecord(
            session,
            mappingRecord.getTargetCatalogId(),
            mappingRecord.getTargetId(),
            mappingRecord.getPolicyTypeCode(),
            mappingRecord.getPolicyCatalogId(),
            mappingRecord.getPolicyId());
    if (model != null) {
      model.update(mappingRecord);
    } else {
      model = ModelPolicyMappingRecord.fromPolicyMappingRecord(mappingRecord);
    }

    session.persist(model);
  }

  void deleteFromPolicyMappingRecords(
      EntityManager session, PolarisPolicyMappingRecord mappingRecord) {
    diagnosticServices.check(session != null, "session_is_null");
    checkInitialized();

    ModelPolicyMappingRecord lookupPolicyMappingRecord =
        lookupPolicyMappingRecord(
            session,
            mappingRecord.getTargetCatalogId(),
            mappingRecord.getTargetId(),
            mappingRecord.getPolicyTypeCode(),
            mappingRecord.getPolicyCatalogId(),
            mappingRecord.getPolicyId());

    diagnosticServices.check(lookupPolicyMappingRecord != null, "policy_mapping_record_not_found");
    session.remove(lookupPolicyMappingRecord);
  }

  void deleteAllEntityPolicyMappingRecords(EntityManager session, PolarisBaseEntity entity) {
    diagnosticServices.check(session != null, "session_is_null");
    checkInitialized();

    if (entity.getType() == PolarisEntityType.POLICY) {
      PolicyEntity policyEntity = PolicyEntity.of(entity);
      loadAllTargetsOnPolicy(
              session,
              policyEntity.getCatalogId(),
              policyEntity.getId(),
              policyEntity.getPolicyTypeCode())
          .forEach(session::remove);
    } else {
      loadAllPoliciesOnTarget(session, entity.getCatalogId(), entity.getId())
          .forEach(session::remove);
    }
  }

  ModelPolicyMappingRecord lookupPolicyMappingRecord(
      EntityManager session,
      long targetCatalogId,
      long targetId,
      int policyTypeCode,
      long policyCatalogId,
      long policyId) {
    diagnosticServices.check(session != null, "session_is_null");
    checkInitialized();

    return session
        .createQuery(
            "SELECT m from ModelPolicyMappingRecord m "
                + "where m.targetCatalogId=:targetCatalogId "
                + "and m.targetId=:targetId "
                + "and m.policyTypeCode=:policyTypeCode "
                + "and m.policyCatalogId=:policyCatalogId "
                + "and m.policyId=:policyId",
            ModelPolicyMappingRecord.class)
        .setParameter("targetCatalogId", targetCatalogId)
        .setParameter("targetId", targetId)
        .setParameter("policyTypeCode", policyTypeCode)
        .setParameter("policyCatalogId", policyCatalogId)
        .setParameter("policyId", policyId)
        .getResultStream()
        .findFirst()
        .orElse(null);
  }

  List<ModelPolicyMappingRecord> loadPoliciesOnTargetByType(
      EntityManager session, long targetCatalogId, long targetId, int policyTypeCode) {
    diagnosticServices.check(session != null, "session_is_null");
    checkInitialized();

    return session
        .createQuery(
            "SELECT m from ModelPolicyMappingRecord m "
                + "where m.targetCatalogId=:targetCatalogId "
                + "and m.targetId=:targetId "
                + "and m.policyTypeCode=:policyTypeCode",
            ModelPolicyMappingRecord.class)
        .setParameter("targetCatalogId", targetCatalogId)
        .setParameter("targetId", targetId)
        .setParameter("policyTypeCode", policyTypeCode)
        .getResultList();
  }

  List<ModelPolicyMappingRecord> loadAllPoliciesOnTarget(
      EntityManager session, long targetCatalogId, long targetId) {
    diagnosticServices.check(session != null, "session_is_null");
    checkInitialized();

    return session
        .createQuery(
            "SELECT m from ModelPolicyMappingRecord m "
                + " where m.targetCatalogId=:targetCatalogId "
                + "and m.targetId=:targetId",
            ModelPolicyMappingRecord.class)
        .setParameter("targetCatalogId", targetCatalogId)
        .setParameter("targetId", targetId)
        .getResultList();
  }

  List<ModelPolicyMappingRecord> loadAllTargetsOnPolicy(
      EntityManager session, long policyCatalogId, long policyId, int policyTypeCode) {
    diagnosticServices.check(session != null, "session_is_null");
    checkInitialized();

    return session
        .createQuery(
            "SELECT m from ModelPolicyMappingRecord m "
                + "where m.policyTypeCode=:policyTypeCode "
                + "and m.policyCatalogId=:policyCatalogId "
                + "and m.policyId=:policyId",
            ModelPolicyMappingRecord.class)
        .setParameter("policyTypeCode", policyTypeCode)
        .setParameter("policyCatalogId", policyCatalogId)
        .setParameter("policyId", policyId)
        .getResultList();
  }

  private void checkInitialized() {
    diagnosticServices.check(this.initialized.get(), "store_not_initialized");
  }
}
