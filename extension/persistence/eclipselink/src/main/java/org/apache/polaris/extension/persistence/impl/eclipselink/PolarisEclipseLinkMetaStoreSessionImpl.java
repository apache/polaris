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

import static org.eclipse.persistence.config.PersistenceUnitProperties.ECLIPSELINK_PERSISTENCE_XML;
import static org.eclipse.persistence.config.PersistenceUnitProperties.JDBC_URL;

import com.google.common.base.Predicates;
import jakarta.persistence.EntityManager;
import jakarta.persistence.EntityManagerFactory;
import jakarta.persistence.EntityTransaction;
import jakarta.persistence.OptimisticLockException;
import jakarta.persistence.Persistence;
import jakarta.persistence.PersistenceException;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisChangeTrackingVersions;
import org.apache.polaris.core.entity.PolarisEntitiesActiveKey;
import org.apache.polaris.core.entity.PolarisEntityActiveRecord;
import org.apache.polaris.core.entity.PolarisEntityCore;
import org.apache.polaris.core.entity.PolarisEntityId;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.PolarisGrantRecord;
import org.apache.polaris.core.entity.PolarisPrincipalSecrets;
import org.apache.polaris.core.exceptions.AlreadyExistsException;
import org.apache.polaris.core.persistence.PolarisMetaStoreManagerImpl;
import org.apache.polaris.core.persistence.PolarisMetaStoreSession;
import org.apache.polaris.core.persistence.RetryOnConcurrencyException;
import org.apache.polaris.core.persistence.models.ModelEntity;
import org.apache.polaris.core.persistence.models.ModelEntityActive;
import org.apache.polaris.core.persistence.models.ModelEntityChangeTracking;
import org.apache.polaris.core.persistence.models.ModelGrantRecord;
import org.apache.polaris.core.persistence.models.ModelPrincipalSecrets;
import org.apache.polaris.core.storage.PolarisStorageConfigurationInfo;
import org.apache.polaris.core.storage.PolarisStorageIntegration;
import org.apache.polaris.core.storage.PolarisStorageIntegrationProvider;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

/**
 * EclipseLink implementation of a Polaris metadata store supporting persisting and retrieving all
 * Polaris metadata from/to the configured database systems.
 */
public class PolarisEclipseLinkMetaStoreSessionImpl implements PolarisMetaStoreSession {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(PolarisEclipseLinkMetaStoreSessionImpl.class);

  // Cache to hold the EntityManagerFactory for each realm. Each realm needs a separate
  // EntityManagerFactory since it connects to different databases
  private static final ConcurrentHashMap<String, EntityManagerFactory> realmFactories =
      new ConcurrentHashMap<>();
  private final EntityManagerFactory emf;
  private final ThreadLocal<EntityManager> localSession = new ThreadLocal<>();
  private final PolarisEclipseLinkStore store;
  private final PolarisStorageIntegrationProvider storageIntegrationProvider;

  /**
   * Create a meta store session against provided realm. Each realm has its own database.
   *
   * @param store Backing store of EclipseLink implementation
   * @param storageIntegrationProvider Storage integration provider
   * @param realmContext Realm context used to communicate with different database.
   * @param confFile Optional EclipseLink configuration file. Default to 'META-INF/persistence.xml'.
   * @param persistenceUnitName Optional persistence-unit name in confFile. Default to 'polaris'.
   */
  public PolarisEclipseLinkMetaStoreSessionImpl(
      @NotNull PolarisEclipseLinkStore store,
      @NotNull PolarisStorageIntegrationProvider storageIntegrationProvider,
      @NotNull RealmContext realmContext,
      @Nullable String confFile,
      @Nullable String persistenceUnitName) {
    LOGGER.debug(
        "Creating EclipseLink Meta Store Session for realm {}", realmContext.getRealmIdentifier());
    emf = createEntityManagerFactory(realmContext, confFile, persistenceUnitName);

    // init store
    this.store = store;
    try (EntityManager session = emf.createEntityManager()) {
      this.store.initialize(session);
    }
    this.storageIntegrationProvider = storageIntegrationProvider;
  }

  /**
   * Create EntityManagerFactory.
   *
   * <p>The EntityManagerFactory creation is expensive, so we are caching and reusing it for each
   * realm.
   */
  private EntityManagerFactory createEntityManagerFactory(
      @NotNull RealmContext realmContext,
      @Nullable String confFile,
      @Nullable String persistenceUnitName) {
    String realm = realmContext.getRealmIdentifier();
    EntityManagerFactory factory = realmFactories.getOrDefault(realm, null);
    if (factory != null) {
      return factory;
    }

    ClassLoader prevClassLoader = Thread.currentThread().getContextClassLoader();
    try {
      persistenceUnitName = persistenceUnitName == null ? "polaris" : persistenceUnitName;
      confFile = confFile == null ? "META-INF/persistence.xml" : confFile;

      // Currently eclipseLink can only support configuration as a resource inside a jar. To support
      // external configuration, persistence.xml needs be placed inside a jar and here is to add the
      // jar to the classpath.
      // Supported configuration file: META-INF/persistence.xml, /tmp/conf.jar!/persistence.xml
      int splitPosition = confFile.indexOf("!/");
      if (splitPosition != -1) {
        String jarPrefixPath = confFile.substring(0, splitPosition);
        confFile = confFile.substring(splitPosition + 2);
        URL prefixUrl = this.getClass().getClassLoader().getResource(jarPrefixPath);
        if (prefixUrl == null) {
          prefixUrl = new File(jarPrefixPath).toURI().toURL();
        }

        LOGGER.debug(
            "Creating a new ClassLoader with the jar {} in classpath to load the config file",
            prefixUrl);

        URLClassLoader currentClassLoader =
            new URLClassLoader(new URL[] {prefixUrl}, this.getClass().getClassLoader());

        LOGGER.debug("Update ClassLoader in current thread temporarily");
        Thread.currentThread().setContextClassLoader(currentClassLoader);
      }

      Map<String, String> properties = loadProperties(confFile, persistenceUnitName);
      // Replace database name in JDBC URL with realm
      if (properties.containsKey(JDBC_URL)) {
        properties.put(JDBC_URL, properties.get(JDBC_URL).replace("{realm}", realm));
      }
      properties.put(ECLIPSELINK_PERSISTENCE_XML, confFile);

      factory = Persistence.createEntityManagerFactory(persistenceUnitName, properties);
      realmFactories.putIfAbsent(realm, factory);

      return factory;
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      Thread.currentThread().setContextClassLoader(prevClassLoader);
    }
  }

  @TestOnly
  static void clearEntityManagerFactories() {
    realmFactories.clear();
  }

  /** Load the persistence unit properties from a given configuration file */
  private Map<String, String> loadProperties(
      @NotNull String confFile, @NotNull String persistenceUnitName) throws IOException {
    try {
      InputStream input =
          Thread.currentThread().getContextClassLoader().getResourceAsStream(confFile);
      DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
      DocumentBuilder builder = factory.newDocumentBuilder();
      Document doc = builder.parse(input);
      XPath xPath = XPathFactory.newInstance().newXPath();
      String expression =
          "/persistence/persistence-unit[@name='" + persistenceUnitName + "']/properties/property";
      NodeList nodeList =
          (NodeList) xPath.compile(expression).evaluate(doc, XPathConstants.NODESET);
      Map<String, String> properties = new HashMap<>();
      for (int i = 0; i < nodeList.getLength(); i++) {
        NamedNodeMap nodeMap = nodeList.item(i).getAttributes();
        properties.put(
            nodeMap.getNamedItem("name").getNodeValue(),
            nodeMap.getNamedItem("value").getNodeValue());
      }

      return properties;
    } catch (XPathExpressionException
        | ParserConfigurationException
        | SAXException
        | IOException e) {
      String str =
          String.format(
              "Cannot find or parse the configuration file %s for persistence-unit %s",
              confFile, persistenceUnitName);
      LOGGER.error(str, e);
      throw new IOException(str);
    }
  }

  /** {@inheritDoc} */
  @Override
  public <T> T runInTransaction(
      @NotNull PolarisCallContext callCtx, @NotNull Supplier<T> transactionCode) {
    callCtx.getDiagServices().check(localSession.get() == null, "cannot nest transaction");

    try (EntityManager session = emf.createEntityManager()) {
      localSession.set(session);
      EntityTransaction tr = session.getTransaction();
      try {
        tr.begin();

        T result = transactionCode.get();

        // Commit when it's not rolled back by the client
        if (session.getTransaction().isActive()) {
          tr.commit();
          LOGGER.debug("transaction committed");
        }

        return result;
      } catch (Exception e) {
        tr.rollback();
        LOGGER.debug("transaction rolled back", e);

        if (e instanceof OptimisticLockException
            || e.getCause() instanceof OptimisticLockException) {
          throw new RetryOnConcurrencyException(e);
        }

        throw e;
      } finally {
        localSession.remove();
      }
    } catch (PersistenceException e) {
      if (e.toString().toLowerCase(Locale.ROOT).contains("duplicate key")) {
        throw new AlreadyExistsException("Duplicate key error when persisting entity", e);
      } else {
        throw e;
      }
    }
  }

  /** {@inheritDoc} */
  @Override
  public void runActionInTransaction(
      @NotNull PolarisCallContext callCtx, @NotNull Runnable transactionCode) {
    callCtx.getDiagServices().check(localSession.get() == null, "cannot nest transaction");

    try (EntityManager session = emf.createEntityManager()) {
      localSession.set(session);
      EntityTransaction tr = session.getTransaction();
      try {
        tr.begin();

        transactionCode.run();

        // Commit when it's not rolled back by the client
        if (session.getTransaction().isActive()) {
          tr.commit();
          LOGGER.debug("transaction committed");
        }
      } catch (Exception e) {
        LOGGER.debug("Rolling back transaction due to an error", e);
        tr.rollback();

        if (e instanceof OptimisticLockException
            || e.getCause() instanceof OptimisticLockException) {
          throw new RetryOnConcurrencyException(e);
        }

        throw e;
      } finally {
        localSession.remove();
      }
    }
  }

  /** {@inheritDoc} */
  @Override
  public <T> T runInReadTransaction(
      @NotNull PolarisCallContext callCtx, @NotNull Supplier<T> transactionCode) {
    // EclipseLink doesn't support readOnly transaction
    return runInTransaction(callCtx, transactionCode);
  }

  /** {@inheritDoc} */
  @Override
  public void runActionInReadTransaction(
      @NotNull PolarisCallContext callCtx, @NotNull Runnable transactionCode) {
    // EclipseLink doesn't support readOnly transaction
    runActionInTransaction(callCtx, transactionCode);
  }

  /**
   * @return new unique entity identifier
   */
  @Override
  public long generateNewId(@NotNull PolarisCallContext callCtx) {
    // This function can be called within a transaction or out of transaction.
    // If called out of transaction, create a new transaction, otherwise run in current transaction
    return localSession.get() != null
        ? this.store.getNextSequence(localSession.get())
        : runInReadTransaction(callCtx, () -> generateNewId(callCtx));
  }

  /** {@inheritDoc} */
  @Override
  public void writeToEntities(
      @NotNull PolarisCallContext callCtx, @NotNull PolarisBaseEntity entity) {
    this.store.writeToEntities(localSession.get(), entity);
  }

  /** {@inheritDoc} */
  @Override
  public <T extends PolarisStorageConfigurationInfo> void persistStorageIntegrationIfNeeded(
      @NotNull PolarisCallContext callContext,
      @NotNull PolarisBaseEntity entity,
      @Nullable PolarisStorageIntegration<T> storageIntegration) {
    // not implemented for eclipselink store
  }

  /** {@inheritDoc} */
  @Override
  public void writeToEntitiesActive(
      @NotNull PolarisCallContext callCtx, @NotNull PolarisBaseEntity entity) {
    // write it
    this.store.writeToEntitiesActive(localSession.get(), entity);
  }

  /** {@inheritDoc} */
  @Override
  public void writeToEntitiesDropped(
      @NotNull PolarisCallContext callCtx, @NotNull PolarisBaseEntity entity) {
    // write it
    this.store.writeToEntitiesDropped(localSession.get(), entity);
  }

  /** {@inheritDoc} */
  @Override
  public void writeToEntitiesChangeTracking(
      @NotNull PolarisCallContext callCtx, @NotNull PolarisBaseEntity entity) {
    // write it
    this.store.writeToEntitiesChangeTracking(localSession.get(), entity);
  }

  /** {@inheritDoc} */
  @Override
  public void writeToGrantRecords(
      @NotNull PolarisCallContext callCtx, @NotNull PolarisGrantRecord grantRec) {
    // write it
    this.store.writeToGrantRecords(localSession.get(), grantRec);
  }

  /** {@inheritDoc} */
  @Override
  public void deleteFromEntities(
      @NotNull PolarisCallContext callCtx, @NotNull PolarisEntityCore entity) {

    // delete it
    this.store.deleteFromEntities(localSession.get(), entity.getCatalogId(), entity.getId());
  }

  /** {@inheritDoc} */
  @Override
  public void deleteFromEntitiesActive(
      @NotNull PolarisCallContext callCtx, @NotNull PolarisEntityCore entity) {
    // delete it
    this.store.deleteFromEntitiesActive(localSession.get(), new PolarisEntitiesActiveKey(entity));
  }

  /** {@inheritDoc} */
  @Override
  public void deleteFromEntitiesDropped(
      @NotNull PolarisCallContext callCtx, @NotNull PolarisBaseEntity entity) {
    // delete it
    this.store.deleteFromEntitiesDropped(localSession.get(), entity.getCatalogId(), entity.getId());
  }

  /**
   * {@inheritDoc}
   *
   * @param callCtx
   * @param entity entity record to delete
   */
  @Override
  public void deleteFromEntitiesChangeTracking(
      @NotNull PolarisCallContext callCtx, @NotNull PolarisEntityCore entity) {
    // delete it
    this.store.deleteFromEntitiesChangeTracking(localSession.get(), entity);
  }

  /** {@inheritDoc} */
  @Override
  public void deleteFromGrantRecords(
      @NotNull PolarisCallContext callCtx, @NotNull PolarisGrantRecord grantRec) {
    this.store.deleteFromGrantRecords(localSession.get(), grantRec);
  }

  /** {@inheritDoc} */
  @Override
  public void deleteAllEntityGrantRecords(
      @NotNull PolarisCallContext callCtx,
      @NotNull PolarisEntityCore entity,
      @NotNull List<PolarisGrantRecord> grantsOnGrantee,
      @NotNull List<PolarisGrantRecord> grantsOnSecurable) {
    this.store.deleteAllEntityGrantRecords(localSession.get(), entity);
  }

  /** {@inheritDoc} */
  @Override
  public void deleteAll(@NotNull PolarisCallContext callCtx) {
    this.store.deleteAll(localSession.get());
  }

  /** {@inheritDoc} */
  @Override
  public @Nullable PolarisBaseEntity lookupEntity(
      @NotNull PolarisCallContext callCtx, long catalogId, long entityId) {
    return ModelEntity.toEntity(this.store.lookupEntity(localSession.get(), catalogId, entityId));
  }

  @Override
  public @NotNull List<PolarisBaseEntity> lookupEntities(
      @NotNull PolarisCallContext callCtx, List<PolarisEntityId> entityIds) {
    return this.store.lookupEntities(localSession.get(), entityIds).stream()
        .map(ModelEntity::toEntity)
        .toList();
  }

  /** {@inheritDoc} */
  @Override
  public int lookupEntityVersion(
      @NotNull PolarisCallContext callCtx, long catalogId, long entityId) {
    ModelEntity model = this.store.lookupEntity(localSession.get(), catalogId, entityId);
    return model == null ? 0 : model.getEntityVersion();
  }

  /** {@inheritDoc} */
  @Override
  public @NotNull List<PolarisChangeTrackingVersions> lookupEntityVersions(
      @NotNull PolarisCallContext callCtx, List<PolarisEntityId> entityIds) {
    Map<PolarisEntityId, ModelEntity> idToEntityMap =
        this.store.lookupEntities(localSession.get(), entityIds).stream()
            .collect(
                Collectors.toMap(
                    entry -> new PolarisEntityId(entry.getCatalogId(), entry.getId()),
                    entry -> entry));
    return entityIds.stream()
        .map(
            entityId -> {
              ModelEntity entity = idToEntityMap.getOrDefault(entityId, null);
              return entity == null
                  ? null
                  : new PolarisChangeTrackingVersions(
                      entity.getEntityVersion(), entity.getGrantRecordsVersion());
            })
        .collect(Collectors.toList());
  }

  /** {@inheritDoc} */
  @Override
  @Nullable
  public PolarisEntityActiveRecord lookupEntityActive(
      @NotNull PolarisCallContext callCtx, @NotNull PolarisEntitiesActiveKey entityActiveKey) {
    // lookup the active entity slice
    return ModelEntityActive.toEntityActive(
        this.store.lookupEntityActive(localSession.get(), entityActiveKey));
  }

  /** {@inheritDoc} */
  @Override
  @NotNull
  public List<PolarisEntityActiveRecord> lookupEntityActiveBatch(
      @NotNull PolarisCallContext callCtx,
      @NotNull List<PolarisEntitiesActiveKey> entityActiveKeys) {
    // now build a list to quickly verify that nothing has changed
    return entityActiveKeys.stream()
        .map(entityActiveKey -> this.lookupEntityActive(callCtx, entityActiveKey))
        .collect(Collectors.toList());
  }

  /** {@inheritDoc} */
  @Override
  public @NotNull List<PolarisEntityActiveRecord> listActiveEntities(
      @NotNull PolarisCallContext callCtx,
      long catalogId,
      long parentId,
      @NotNull PolarisEntityType entityType) {
    return listActiveEntities(callCtx, catalogId, parentId, entityType, Predicates.alwaysTrue());
  }

  @Override
  public @NotNull List<PolarisEntityActiveRecord> listActiveEntities(
      @NotNull PolarisCallContext callCtx,
      long catalogId,
      long parentId,
      @NotNull PolarisEntityType entityType,
      @NotNull Predicate<PolarisBaseEntity> entityFilter) {
    // full range scan under the parent for that type
    return listActiveEntities(
        callCtx,
        catalogId,
        parentId,
        entityType,
        Integer.MAX_VALUE,
        entityFilter,
        entity ->
            new PolarisEntityActiveRecord(
                entity.getCatalogId(),
                entity.getId(),
                entity.getParentId(),
                entity.getName(),
                entity.getTypeCode(),
                entity.getSubTypeCode()));
  }

  @Override
  public @NotNull <T> List<T> listActiveEntities(
      @NotNull PolarisCallContext callCtx,
      long catalogId,
      long parentId,
      @NotNull PolarisEntityType entityType,
      int limit,
      @NotNull Predicate<PolarisBaseEntity> entityFilter,
      @NotNull Function<PolarisBaseEntity, T> transformer) {
    // full range scan under the parent for that type
    return this.store
        .lookupFullEntitiesActive(localSession.get(), catalogId, parentId, entityType)
        .stream()
        .map(ModelEntity::toEntity)
        .filter(entityFilter)
        .limit(limit)
        .map(transformer)
        .collect(Collectors.toList());
  }

  /** {@inheritDoc} */
  @Override
  public boolean hasChildren(
      @NotNull PolarisCallContext callContext,
      @Nullable PolarisEntityType entityType,
      long catalogId,
      long parentId) {
    // check if it has children
    return this.store.countActiveChildEntities(localSession.get(), catalogId, parentId, entityType)
        > 0;
  }

  /** {@inheritDoc} */
  @Override
  public int lookupEntityGrantRecordsVersion(
      @NotNull PolarisCallContext callCtx, long catalogId, long entityId) {
    ModelEntityChangeTracking entity =
        this.store.lookupEntityChangeTracking(localSession.get(), catalogId, entityId);

    // does not exist, 0
    return entity == null ? 0 : entity.getGrantRecordsVersion();
  }

  /** {@inheritDoc} */
  @Override
  public @Nullable PolarisGrantRecord lookupGrantRecord(
      @NotNull PolarisCallContext callCtx,
      long securableCatalogId,
      long securableId,
      long granteeCatalogId,
      long granteeId,
      int privilegeCode) {
    // lookup the grants records slice to find the usage role
    return ModelGrantRecord.toGrantRecord(
        this.store.lookupGrantRecord(
            localSession.get(),
            securableCatalogId,
            securableId,
            granteeCatalogId,
            granteeId,
            privilegeCode));
  }

  /** {@inheritDoc} */
  @Override
  public @NotNull List<PolarisGrantRecord> loadAllGrantRecordsOnSecurable(
      @NotNull PolarisCallContext callCtx, long securableCatalogId, long securableId) {
    // now fetch all grants for this securable
    return this.store
        .lookupAllGrantRecordsOnSecurable(localSession.get(), securableCatalogId, securableId)
        .stream()
        .map(ModelGrantRecord::toGrantRecord)
        .toList();
  }

  /** {@inheritDoc} */
  @Override
  public @NotNull List<PolarisGrantRecord> loadAllGrantRecordsOnGrantee(
      @NotNull PolarisCallContext callCtx, long granteeCatalogId, long granteeId) {
    // now fetch all grants assigned to this grantee
    return this.store
        .lookupGrantRecordsOnGrantee(localSession.get(), granteeCatalogId, granteeId)
        .stream()
        .map(ModelGrantRecord::toGrantRecord)
        .toList();
  }

  /** {@inheritDoc} */
  @Override
  public @Nullable PolarisPrincipalSecrets loadPrincipalSecrets(
      @NotNull PolarisCallContext callCtx, @NotNull String clientId) {
    return ModelPrincipalSecrets.toPrincipalSecrets(
        this.store.lookupPrincipalSecrets(localSession.get(), clientId));
  }

  /** {@inheritDoc} */
  @Override
  public @NotNull PolarisPrincipalSecrets generateNewPrincipalSecrets(
      @NotNull PolarisCallContext callCtx, @NotNull String principalName, long principalId) {
    // ensure principal client id is unique
    PolarisPrincipalSecrets principalSecrets;
    ModelPrincipalSecrets lookupPrincipalSecrets;
    do {
      // generate new random client id and secrets
      principalSecrets = new PolarisPrincipalSecrets(principalId);

      // load the existing secrets
      lookupPrincipalSecrets =
          this.store.lookupPrincipalSecrets(
              localSession.get(), principalSecrets.getPrincipalClientId());
    } while (lookupPrincipalSecrets != null);

    // write new principal secrets
    this.store.writePrincipalSecrets(localSession.get(), principalSecrets);

    // if not found, return null
    return principalSecrets;
  }

  /** {@inheritDoc} */
  @Override
  public @NotNull PolarisPrincipalSecrets rotatePrincipalSecrets(
      @NotNull PolarisCallContext callCtx,
      @NotNull String clientId,
      long principalId,
      boolean reset,
      @NotNull String oldSecretHash) {

    // load the existing secrets
    PolarisPrincipalSecrets principalSecrets =
        ModelPrincipalSecrets.toPrincipalSecrets(
            this.store.lookupPrincipalSecrets(localSession.get(), clientId));

    // should be found
    callCtx
        .getDiagServices()
        .checkNotNull(
            principalSecrets,
            "cannot_find_secrets",
            "client_id={} principalId={}",
            clientId,
            principalId);

    // ensure principal id is matching
    callCtx
        .getDiagServices()
        .check(
            principalId == principalSecrets.getPrincipalId(),
            "principal_id_mismatch",
            "expectedId={} id={}",
            principalId,
            principalSecrets.getPrincipalId());

    // rotate the secrets
    principalSecrets.rotateSecrets(oldSecretHash);
    if (reset) {
      principalSecrets.rotateSecrets(principalSecrets.getMainSecretHash());
    }

    // write back new secrets
    this.store.writePrincipalSecrets(localSession.get(), principalSecrets);

    // return those
    return principalSecrets;
  }

  /** {@inheritDoc} */
  @Override
  public void deletePrincipalSecrets(
      @NotNull PolarisCallContext callCtx, @NotNull String clientId, long principalId) {
    // load the existing secrets
    ModelPrincipalSecrets principalSecrets =
        this.store.lookupPrincipalSecrets(localSession.get(), clientId);

    // should be found
    callCtx
        .getDiagServices()
        .checkNotNull(
            principalSecrets,
            "cannot_find_secrets",
            "client_id={} principalId={}",
            clientId,
            principalId);

    // ensure principal id is matching
    callCtx
        .getDiagServices()
        .check(
            principalId == principalSecrets.getPrincipalId(),
            "principal_id_mismatch",
            "expectedId={} id={}",
            principalId,
            principalSecrets.getPrincipalId());

    // delete these secrets
    this.store.deletePrincipalSecrets(localSession.get(), clientId);
  }

  /** {@inheritDoc} */
  @Override
  public @Nullable <T extends PolarisStorageConfigurationInfo>
      PolarisStorageIntegration<T> createStorageIntegration(
          @NotNull PolarisCallContext callCtx,
          long catalogId,
          long entityId,
          PolarisStorageConfigurationInfo polarisStorageConfigurationInfo) {
    return storageIntegrationProvider.getStorageIntegrationForConfig(
        polarisStorageConfigurationInfo);
  }

  /** {@inheritDoc} */
  @Override
  public @Nullable <T extends PolarisStorageConfigurationInfo>
      PolarisStorageIntegration<T> loadPolarisStorageIntegration(
          @NotNull PolarisCallContext callCtx, @NotNull PolarisBaseEntity entity) {
    PolarisStorageConfigurationInfo storageConfig =
        PolarisMetaStoreManagerImpl.readStorageConfiguration(callCtx, entity);
    return storageIntegrationProvider.getStorageIntegrationForConfig(storageConfig);
  }

  @Override
  public void rollback() {
    EntityManager session = localSession.get();
    if (session != null) {
      session.getTransaction().rollback();
    }
  }
}
