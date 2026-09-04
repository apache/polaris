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
package org.apache.polaris.persistence.relational.jdbc;

import static org.apache.polaris.core.persistence.PrincipalSecretsGenerator.RANDOM_SECRETS;

import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.sql.DataSource;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.PolarisDefaultDiagServiceImpl;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.entity.LocationBasedEntity;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PolarisEntityConstants;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.persistence.AtomicOperationMetaStoreManager;
import org.apache.polaris.core.persistence.BasePolarisMetaStoreManagerTest;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.core.persistence.PolarisTestMetaStoreManager;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.Assumptions;
import org.h2.jdbcx.JdbcConnectionPool;
import org.junit.jupiter.api.Test;

public abstract class AtomicMetastoreManagerWithJdbcBasePersistenceImplTest
    extends BasePolarisMetaStoreManagerTest {

  protected DatabaseType databaseType() {
    return DatabaseType.H2;
  }

  public abstract int schemaVersion();

  protected DataSource createDataSource() {
    // The schema is provided by the datasource, not by the persistence code: INIT creates the
    // schema (the DBA step in a real deployment) and selects it as the session schema on every
    // connection (the currentSchema/SCHEMA driver setting in a real deployment).
    return JdbcConnectionPool.create(
        String.format(
            "jdbc:h2:file:./build/test_data/polaris/db_%s_%d"
                + ";INIT=CREATE SCHEMA IF NOT EXISTS POLARIS_SCHEMA\\;SET SCHEMA POLARIS_SCHEMA",
            databaseType().getDisplayName(), schemaVersion()),
        "sa",
        "");
  }

  protected InputStream openSchemaScript() {
    ClassLoader classLoader = DatasourceOperations.class.getClassLoader();
    String resource =
        String.format("%s/schema-v%d.sql", databaseType().getDisplayName(), schemaVersion());
    InputStream scriptStream = classLoader.getResourceAsStream(resource);
    if (scriptStream == null) {
      throw new IllegalStateException("Schema resource not found: " + resource);
    }
    return scriptStream;
  }

  @Override
  protected PolarisTestMetaStoreManager createPolarisTestMetaStoreManager() {
    PolarisDiagnostics diagServices = new PolarisDefaultDiagServiceImpl();
    DatasourceOperations datasourceOperations;
    try {
      datasourceOperations =
          new DatasourceOperations(
              createDataSource(),
              SimpleRelationalJdbcConfiguration.forDatabaseType(databaseType()));
      try (InputStream scriptStream = openSchemaScript()) {
        datasourceOperations.executeScript(scriptStream);
      }
    } catch (SQLException | IOException e) {
      throw new RuntimeException(
          String.format(
              "Error executing %s schema-v%d script: %s",
              databaseType(), schemaVersion(), e.getMessage()),
          e);
    }

    RealmContext realmContext = () -> "REALM";
    JdbcBasePersistenceImpl basePersistence =
        new JdbcBasePersistenceImpl(
            diagServices,
            datasourceOperations,
            RANDOM_SECRETS,
            realmContext.getRealmIdentifier(),
            schemaVersion());
    AtomicOperationMetaStoreManager metaStoreManager =
        new AtomicOperationMetaStoreManager(clock, diagServices);
    PolarisCallContext callCtx = new PolarisCallContext(realmContext, basePersistence);
    return new PolarisTestMetaStoreManager(metaStoreManager, callCtx);
  }

  @Test
  void testHasOverlappingSiblingsUsesStoredBaseLocation() {
    // The optimized check relies on the location_without_scheme column added in schema v2.
    Assumptions.assumeThat(schemaVersion()).isGreaterThanOrEqualTo(2);

    var metaStoreManager = polarisTestMetaStoreManager.polarisMetaStoreManager();
    var callContext = polarisTestMetaStoreManager.polarisCallContext();
    PolarisBaseEntity catalog =
        new PolarisBaseEntity(
            PolarisEntityConstants.getNullId(),
            metaStoreManager.generateNewEntityId(callContext).getId(),
            PolarisEntityType.CATALOG,
            PolarisEntitySubType.NULL_SUBTYPE,
            PolarisEntityConstants.getRootEntityId(),
            "overlap_test_catalog");
    catalog = metaStoreManager.createCatalog(callContext, catalog, List.of()).getCatalog();

    PolarisBaseEntity existingNamespace =
        buildLocationBasedNamespace(
            catalog,
            metaStoreManager.generateNewEntityId(callContext).getId(),
            "existing",
            "s3://bucket/warehouse/existing/");
    metaStoreManager.createEntityIfNotExists(
        callContext, List.of(PolarisEntity.toCore(catalog)), existingNamespace);

    TestLocationBasedEntity candidateNamespace =
        new TestLocationBasedEntity(
            buildLocationBasedNamespace(
                catalog,
                metaStoreManager.generateNewEntityId(callContext).getId(),
                "candidate",
                "s3://bucket/warehouse/existing/child"));

    Assertions.assertThat(metaStoreManager.hasOverlappingSiblings(callContext, candidateNamespace))
        .contains(Optional.of("s3://bucket/warehouse/existing/"));

    TestLocationBasedEntity nonOverlappingNamespace =
        new TestLocationBasedEntity(
            buildLocationBasedNamespace(
                catalog,
                metaStoreManager.generateNewEntityId(callContext).getId(),
                "non_overlapping",
                "s3://bucket/warehouse/non-overlapping"));

    Assertions.assertThat(
            metaStoreManager.hasOverlappingSiblings(callContext, nonOverlappingNamespace))
        .contains(Optional.empty());
  }

  @Test
  void testHasOverlappingSiblingsTrailingSlashVariants() {
    Assumptions.assumeThat(schemaVersion()).isGreaterThanOrEqualTo(2);

    var metaStoreManager = polarisTestMetaStoreManager.polarisMetaStoreManager();
    var callContext = polarisTestMetaStoreManager.polarisCallContext();
    PolarisBaseEntity catalog = createOverlapTestCatalog(metaStoreManager, callContext, "slash");

    PolarisBaseEntity nsWithSlash =
        buildLocationBasedNamespace(
            catalog,
            metaStoreManager.generateNewEntityId(callContext).getId(),
            "ns_slash",
            "s3://bucket/foo/bar/");
    metaStoreManager.createEntityIfNotExists(
        callContext, List.of(PolarisEntity.toCore(catalog)), nsWithSlash);

    // Candidate without trailing slash should still detect overlap
    TestLocationBasedEntity candidateNoSlash =
        new TestLocationBasedEntity(
            buildLocationBasedNamespace(
                catalog,
                metaStoreManager.generateNewEntityId(callContext).getId(),
                "candidate_no_slash",
                "s3://bucket/foo/bar"));

    Assertions.assertThat(metaStoreManager.hasOverlappingSiblings(callContext, candidateNoSlash))
        .as("Location without trailing slash must overlap with stored trailing-slash location")
        .isPresent()
        .contains(Optional.of("s3://bucket/foo/bar/"));
  }

  @Test
  void testHasOverlappingSiblingsParentChildBothDirections() {
    Assumptions.assumeThat(schemaVersion()).isGreaterThanOrEqualTo(2);

    var metaStoreManager = polarisTestMetaStoreManager.polarisMetaStoreManager();
    var callContext = polarisTestMetaStoreManager.polarisCallContext();
    PolarisBaseEntity catalog =
        createOverlapTestCatalog(metaStoreManager, callContext, "parent_child");

    PolarisBaseEntity parentNs =
        buildLocationBasedNamespace(
            catalog,
            metaStoreManager.generateNewEntityId(callContext).getId(),
            "parent_ns",
            "s3://bucket/data/");
    metaStoreManager.createEntityIfNotExists(
        callContext, List.of(PolarisEntity.toCore(catalog)), parentNs);

    // Child of existing parent must report overlap
    TestLocationBasedEntity childCandidate =
        new TestLocationBasedEntity(
            buildLocationBasedNamespace(
                catalog,
                metaStoreManager.generateNewEntityId(callContext).getId(),
                "child_candidate",
                "s3://bucket/data/nested/deep/"));

    Assertions.assertThat(metaStoreManager.hasOverlappingSiblings(callContext, childCandidate))
        .as("Child location must detect parent overlap")
        .isPresent()
        .contains(Optional.of("s3://bucket/data/"));

    // Now add a deeply nested entity and verify a broader parent candidate detects it
    PolarisBaseEntity deepNs =
        buildLocationBasedNamespace(
            catalog,
            metaStoreManager.generateNewEntityId(callContext).getId(),
            "deep_ns",
            "s3://bucket/warehouse/a/b/c/");
    metaStoreManager.createEntityIfNotExists(
        callContext, List.of(PolarisEntity.toCore(catalog)), deepNs);

    TestLocationBasedEntity parentCandidate =
        new TestLocationBasedEntity(
            buildLocationBasedNamespace(
                catalog,
                metaStoreManager.generateNewEntityId(callContext).getId(),
                "parent_candidate",
                "s3://bucket/warehouse/a/"));

    Assertions.assertThat(metaStoreManager.hasOverlappingSiblings(callContext, parentCandidate))
        .as("Parent location must detect existing child overlap")
        .isPresent()
        .contains(Optional.of("s3://bucket/warehouse/a/b/c/"));
  }

  @Test
  void testHasOverlappingSiblingsDuplicateLocations() {
    Assumptions.assumeThat(schemaVersion()).isGreaterThanOrEqualTo(2);

    var metaStoreManager = polarisTestMetaStoreManager.polarisMetaStoreManager();
    var callContext = polarisTestMetaStoreManager.polarisCallContext();
    PolarisBaseEntity catalog =
        createOverlapTestCatalog(metaStoreManager, callContext, "duplicate");

    PolarisBaseEntity ns1 =
        buildLocationBasedNamespace(
            catalog,
            metaStoreManager.generateNewEntityId(callContext).getId(),
            "dup1",
            "s3://bucket/shared/path/");
    metaStoreManager.createEntityIfNotExists(
        callContext, List.of(PolarisEntity.toCore(catalog)), ns1);

    PolarisBaseEntity ns2 =
        buildLocationBasedNamespace(
            catalog,
            metaStoreManager.generateNewEntityId(callContext).getId(),
            "dup2",
            "s3://bucket/shared/path/");
    metaStoreManager.createEntityIfNotExists(
        callContext, List.of(PolarisEntity.toCore(catalog)), ns2);

    // A candidate with the same location must detect the overlap
    TestLocationBasedEntity sameLocationCandidate =
        new TestLocationBasedEntity(
            buildLocationBasedNamespace(
                catalog,
                metaStoreManager.generateNewEntityId(callContext).getId(),
                "same_loc",
                "s3://bucket/shared/path/"));

    Assertions.assertThat(
            metaStoreManager.hasOverlappingSiblings(callContext, sameLocationCandidate))
        .as("Exact duplicate location must report overlap")
        .isPresent()
        .contains(Optional.of("s3://bucket/shared/path/"));
  }

  @Test
  void testHasOverlappingSiblingsAfterEntityDrop() {
    Assumptions.assumeThat(schemaVersion()).isGreaterThanOrEqualTo(2);

    var metaStoreManager = polarisTestMetaStoreManager.polarisMetaStoreManager();
    var callContext = polarisTestMetaStoreManager.polarisCallContext();
    PolarisBaseEntity catalog = createOverlapTestCatalog(metaStoreManager, callContext, "drop");

    PolarisBaseEntity ns1 =
        buildLocationBasedNamespace(
            catalog,
            metaStoreManager.generateNewEntityId(callContext).getId(),
            "drop_ns1",
            "s3://bucket/drop/path/");
    metaStoreManager.createEntityIfNotExists(
        callContext, List.of(PolarisEntity.toCore(catalog)), ns1);

    PolarisBaseEntity ns2 =
        buildLocationBasedNamespace(
            catalog,
            metaStoreManager.generateNewEntityId(callContext).getId(),
            "drop_ns2",
            "s3://bucket/drop/path/");
    metaStoreManager.createEntityIfNotExists(
        callContext, List.of(PolarisEntity.toCore(catalog)), ns2);

    // Drop the first entity
    metaStoreManager.dropEntityIfExists(
        callContext, List.of(PolarisEntity.toCore(catalog)), ns1, null, false);

    // Overlap must still be reported because ns2 remains
    TestLocationBasedEntity candidate =
        new TestLocationBasedEntity(
            buildLocationBasedNamespace(
                catalog,
                metaStoreManager.generateNewEntityId(callContext).getId(),
                "after_drop",
                "s3://bucket/drop/path/child/"));

    Assertions.assertThat(metaStoreManager.hasOverlappingSiblings(callContext, candidate))
        .as("Overlap must persist while a duplicate entity remains")
        .isPresent()
        .contains(Optional.of("s3://bucket/drop/path/"));

    // Drop the second entity
    metaStoreManager.dropEntityIfExists(
        callContext, List.of(PolarisEntity.toCore(catalog)), ns2, null, false);

    // No overlap should be reported now
    Assertions.assertThat(metaStoreManager.hasOverlappingSiblings(callContext, candidate))
        .as("No overlap after all matching entities are dropped")
        .isPresent()
        .contains(Optional.empty());
  }

  private PolarisBaseEntity createOverlapTestCatalog(
      PolarisMetaStoreManager metaStoreManager, PolarisCallContext callContext, String suffix) {
    PolarisBaseEntity catalog =
        new PolarisBaseEntity(
            PolarisEntityConstants.getNullId(),
            metaStoreManager.generateNewEntityId(callContext).getId(),
            PolarisEntityType.CATALOG,
            PolarisEntitySubType.NULL_SUBTYPE,
            PolarisEntityConstants.getRootEntityId(),
            "overlap_" + suffix);
    return metaStoreManager.createCatalog(callContext, catalog, List.of()).getCatalog();
  }

  private static PolarisBaseEntity buildLocationBasedNamespace(
      PolarisBaseEntity catalog, long entityId, String name, String baseLocation) {
    return new PolarisBaseEntity.Builder()
        .catalogId(catalog.getId())
        .parentId(catalog.getId())
        .id(entityId)
        .typeCode(PolarisEntityType.NAMESPACE.getCode())
        .subTypeCode(PolarisEntitySubType.NULL_SUBTYPE.getCode())
        .name(name)
        .propertiesAsMap(Map.of(PolarisEntityConstants.ENTITY_BASE_LOCATION, baseLocation))
        .internalPropertiesAsMap(Map.of())
        .build();
  }

  private static class TestLocationBasedEntity extends PolarisEntity
      implements LocationBasedEntity {
    TestLocationBasedEntity(PolarisBaseEntity sourceEntity) {
      super(sourceEntity);
    }

    @Override
    public String getBaseLocation() {
      return getPropertiesAsMap().get(PolarisEntityConstants.ENTITY_BASE_LOCATION);
    }
  }
}
