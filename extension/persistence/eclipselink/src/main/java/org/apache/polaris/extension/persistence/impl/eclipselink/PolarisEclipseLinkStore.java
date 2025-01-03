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
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntitiesActiveKey;
import org.apache.polaris.core.entity.PolarisEntityActiveRecord;
import org.apache.polaris.core.entity.PolarisEntityCore;
import org.apache.polaris.core.entity.PolarisEntityId;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.PolarisGrantRecord;
import org.apache.polaris.core.entity.PolarisPrincipalSecrets;
import org.apache.polaris.jpa.models.ModelEntity;
import org.apache.polaris.jpa.models.ModelEntityActive;
import org.apache.polaris.jpa.models.ModelEntityChangeTracking;
import org.apache.polaris.jpa.models.ModelEntityDropped;
import org.apache.polaris.jpa.models.ModelGrantRecord;
import org.apache.polaris.jpa.models.ModelPrincipalSecrets;
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

    ModelEntity model = lookupEntity(session, entity.getCatalogId(), entity.getId());
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
      session.persist(ModelEntityActive.fromEntityActive(new PolarisEntityActiveRecord(entity)));
    }
  }

  void writeToEntitiesDropped(EntityManager session, PolarisBaseEntity entity) {
    diagnosticServices.check(session != null, "session_is_null");
    checkInitialized();

    ModelEntityDropped entityDropped =
        lookupEntityDropped(session, entity.getCatalogId(), entity.getId());
    if (entityDropped == null) {
      session.persist(ModelEntityDropped.fromEntity(entity));
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

  void deleteFromEntities(EntityManager session, long catalogId, long entityId) {
    diagnosticServices.check(session != null, "session_is_null");
    checkInitialized();

    ModelEntity model = lookupEntity(session, catalogId, entityId);
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

  void deleteFromEntitiesDropped(EntityManager session, long catalogId, long entityId) {
    diagnosticServices.check(session != null, "session_is_null");
    checkInitialized();

    ModelEntityDropped entity = lookupEntityDropped(session, catalogId, entityId);
    diagnosticServices.check(entity != null, "dropped_entity_not_found");

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
    session.createQuery("DELETE from ModelEntityDropped").executeUpdate();
    session.createQuery("DELETE from ModelEntityChangeTracking").executeUpdate();
    session.createQuery("DELETE from ModelGrantRecord").executeUpdate();
    session.createQuery("DELETE from ModelPrincipalSecrets").executeUpdate();

    LOGGER.debug("All entities deleted.");
  }

  ModelEntity lookupEntity(EntityManager session, long catalogId, long entityId) {
    diagnosticServices.check(session != null, "session_is_null");
    checkInitialized();

    return session
        .createQuery(
            "SELECT m from ModelEntity m where m.catalogId=:catalogId and m.id=:id",
            ModelEntity.class)
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
      EntityManager session, long catalogId, long parentId, @Nonnull PolarisEntityType entityType) {
    diagnosticServices.check(session != null, "session_is_null");
    checkInitialized();

    // Currently check against ENTITIES not joining with ENTITIES_ACTIVE
    String hql =
        "SELECT m from ModelEntity m where m.catalogId=:catalogId and m.parentId=:parentId and m.typeCode=:typeCode";

    TypedQuery<ModelEntity> query =
        session
            .createQuery(hql, ModelEntity.class)
            .setParameter("catalogId", catalogId)
            .setParameter("parentId", parentId)
            .setParameter("typeCode", entityType.getCode());

    return query.getResultList();
  }

  ModelEntityDropped lookupEntityDropped(EntityManager session, long catalogId, long entityId) {
    diagnosticServices.check(session != null, "session_is_null");
    checkInitialized();

    return session
        .createQuery(
            "SELECT m from ModelEntityDropped m where m.catalogId=:catalogId and m.id=:id",
            ModelEntityDropped.class)
        .setParameter("catalogId", catalogId)
        .setParameter("id", entityId)
        .getResultStream()
        .findFirst()
        .orElse(null);
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

  private void checkInitialized() {
    diagnosticServices.check(this.initialized.get(), "store_not_initialized");
  }
}
