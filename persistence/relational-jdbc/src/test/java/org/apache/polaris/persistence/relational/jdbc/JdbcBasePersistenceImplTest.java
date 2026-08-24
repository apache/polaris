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
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iceberg.exceptions.BadRequestException;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.PolarisDefaultDiagServiceImpl;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.entity.EntityNameLookupRecord;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisChangeTrackingVersions;
import org.apache.polaris.core.entity.PolarisEntityId;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.PolarisGrantRecord;
import org.apache.polaris.core.persistence.AtomicOperationMetaStoreManager;
import org.apache.polaris.core.persistence.EntityAlreadyExistsException;
import org.apache.polaris.core.persistence.RetryOnConcurrencyException;
import org.apache.polaris.core.persistence.dao.entity.BaseResult;
import org.apache.polaris.core.persistence.dao.entity.TagAssignmentResult;
import org.apache.polaris.core.persistence.pagination.Page;
import org.apache.polaris.core.persistence.pagination.PageToken;
import org.apache.polaris.core.policy.PolarisPolicyMappingRecord;
import org.apache.polaris.core.policy.PredefinedPolicyTypes;
import org.apache.polaris.core.tag.TagAssignmentRecord;
import org.apache.polaris.core.tag.TagEntity;
import org.apache.polaris.core.tag.exceptions.NoSuchTagException;
import org.apache.polaris.persistence.relational.jdbc.models.ModelEntity;
import org.apache.polaris.persistence.relational.jdbc.models.ModelTagAssignmentRecord;
import org.h2.jdbcx.JdbcConnectionPool;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

class JdbcBasePersistenceImplTest {

  private static final RealmContext REALM_CONTEXT = () -> "REALM";

  private static final long SECURABLE_CATALOG_ID = 1L;
  private static final long SECURABLE_ID = 2L;
  private static final long GRANTEE_CATALOG_ID = 3L;
  private static final long GRANTEE_ID = 4L;
  private static final int PRIVILEGE_CODE = 21;
  private static final long POLICY_TARGET_CATALOG_ID = 11L;
  private static final long POLICY_TARGET_ID = 12L;
  private static final long POLICY_CATALOG_ID = 13L;
  private static final long POLICY_ID = 14L;

  @ParameterizedTest
  @ValueSource(ints = {1, 2, 3, 4, 5})
  void writeToGrantRecordsIsIdempotent(int schemaVersion) throws SQLException {
    JdbcConnectionPool dataSource =
        JdbcConnectionPool.create(
            "jdbc:h2:mem:grant_idempotency_v"
                + schemaVersion
                + "_"
                + System.nanoTime()
                + ";DB_CLOSE_DELAY=-1",
            "sa",
            "");
    DatasourceOperations datasourceOperations =
        new DatasourceOperations(dataSource, new TestJdbcConfiguration());
    try (InputStream scriptStream = DatabaseType.H2.openInitScriptResource(schemaVersion)) {
      datasourceOperations.executeScript(scriptStream);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    JdbcBasePersistenceImpl basePersistence =
        new JdbcBasePersistenceImpl(
            new PolarisDefaultDiagServiceImpl(),
            datasourceOperations,
            RANDOM_SECRETS,
            REALM_CONTEXT.getRealmIdentifier(),
            schemaVersion);
    PolarisCallContext callCtx = new PolarisCallContext(REALM_CONTEXT, basePersistence);

    PolarisGrantRecord grant =
        new PolarisGrantRecord(
            SECURABLE_CATALOG_ID, SECURABLE_ID, GRANTEE_CATALOG_ID, GRANTEE_ID, PRIVILEGE_CODE);

    assertThatCode(() -> basePersistence.writeToGrantRecords(callCtx, grant))
        .doesNotThrowAnyException();
    assertThatCode(() -> basePersistence.writeToGrantRecords(callCtx, grant))
        .doesNotThrowAnyException();
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "23502", // NOT NULL violation
        "23503", // FK violation
        "23513", // CHECK violation
        "08006", // connection failure
        "40001" // serialization failure
      })
  void writeToGrantRecords_IdempotencyCheck_OnlyCatchesPKUniqueness(String sqlStateCode)
      throws SQLException {
    SQLException nonUniqueViolation = new SQLException(sqlStateCode, sqlStateCode);

    DatasourceOperations datasourceOperations = Mockito.mock(DatasourceOperations.class);
    when(datasourceOperations.getDatabaseType()).thenReturn(DatabaseType.H2);
    doThrow(nonUniqueViolation)
        .when(datasourceOperations)
        .executeUpdate(any(QueryGenerator.PreparedQuery.class));
    doCallRealMethod()
        .when(datasourceOperations)
        .isUniquenessConstraintViolation(any(SQLException.class));

    int schemaVersion = 4;
    JdbcBasePersistenceImpl basePersistence =
        new JdbcBasePersistenceImpl(
            new PolarisDefaultDiagServiceImpl(),
            datasourceOperations,
            RANDOM_SECRETS,
            REALM_CONTEXT.getRealmIdentifier(),
            schemaVersion);
    PolarisCallContext callCtx = new PolarisCallContext(REALM_CONTEXT, basePersistence);

    PolarisGrantRecord grant =
        new PolarisGrantRecord(
            SECURABLE_CATALOG_ID, SECURABLE_ID, GRANTEE_CATALOG_ID, GRANTEE_ID, PRIVILEGE_CODE);

    assertThatExceptionOfType(RuntimeException.class)
        .isThrownBy(() -> basePersistence.writeToGrantRecords(callCtx, grant))
        .withMessageContaining("Failed to write to grant records")
        .withCause(nonUniqueViolation);
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 2, 3, 4})
  void writeEntity_uniquenessViolationWithInvisibleConflict_throwsRetryOnConcurrencyException(
      int schemaVersion) throws SQLException {
    DatasourceOperations datasourceOperations = Mockito.mock(DatasourceOperations.class);
    when(datasourceOperations.getDatabaseType()).thenReturn(DatabaseType.H2);
    doThrow(new SQLException("Unique constraint violation", "23505"))
        .when(datasourceOperations)
        .executeUpdate(any(QueryGenerator.PreparedQuery.class));
    Mockito.<List<?>>when(datasourceOperations.executeSelect(any(), any())).thenReturn(List.of());
    doCallRealMethod()
        .when(datasourceOperations)
        .isUniquenessConstraintViolation(any(SQLException.class));

    JdbcBasePersistenceImpl basePersistence =
        new JdbcBasePersistenceImpl(
            new PolarisDefaultDiagServiceImpl(),
            datasourceOperations,
            RANDOM_SECRETS,
            REALM_CONTEXT.getRealmIdentifier(),
            schemaVersion);
    PolarisCallContext callCtx = new PolarisCallContext(REALM_CONTEXT, basePersistence);

    PolarisBaseEntity entity =
        new PolarisBaseEntity.Builder()
            .id(101L)
            .catalogId(0L)
            .parentId(0L)
            .typeCode(PolarisEntityType.PRINCIPAL.getCode())
            .subTypeCode(PolarisEntitySubType.NULL_SUBTYPE.getCode())
            .name("invisible_conflict_entity")
            .entityVersion(1)
            .grantRecordsVersion(0)
            .createTimestamp(System.currentTimeMillis())
            .build();

    assertThatExceptionOfType(RetryOnConcurrencyException.class)
        .isThrownBy(() -> basePersistence.writeEntity(callCtx, entity, true, null))
        .withMessageContaining("not visible");
  }

  @Test
  void withRetries_propagatesRetryOnConcurrencyExceptionWithoutUnwrapping() throws SQLException {
    JdbcConnectionPool dataSource =
        JdbcConnectionPool.create(
            "jdbc:h2:mem:with_retries_concurrency_" + System.nanoTime(), "sa", "");
    DatasourceOperations datasourceOperations =
        new DatasourceOperations(dataSource, new TestJdbcConfiguration());
    RetryOnConcurrencyException expected = new RetryOnConcurrencyException("concurrency conflict");

    assertThatExceptionOfType(RetryOnConcurrencyException.class)
        .isThrownBy(
            () ->
                datasourceOperations.withRetries(
                    () -> {
                      throw expected;
                    }))
        .isSameAs(expected);
  }

  @Test
  void withRetries_propagatesEntityAlreadyExistsExceptionWithoutUnwrapping() throws SQLException {
    JdbcConnectionPool dataSource =
        JdbcConnectionPool.create(
            "jdbc:h2:mem:with_retries_already_exists_" + System.nanoTime(), "sa", "");
    DatasourceOperations datasourceOperations =
        new DatasourceOperations(dataSource, new TestJdbcConfiguration());
    PolarisBaseEntity existing = Mockito.mock(PolarisBaseEntity.class);
    when(existing.getName()).thenReturn("existing");
    when(existing.getId()).thenReturn(1L);
    EntityAlreadyExistsException expected = new EntityAlreadyExistsException(existing);

    assertThatExceptionOfType(EntityAlreadyExistsException.class)
        .isThrownBy(
            () ->
                datasourceOperations.withRetries(
                    () -> {
                      throw expected;
                    }))
        .isSameAs(expected);
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 2})
  void lookupEntityVersionsPreservesOrderAndNullsForMissing(int schemaVersion)
      throws SQLException, IOException {
    DatasourceOperations real = newH2DatasourceOperations("lookup_versions_v", schemaVersion);
    // Spy so we can assert on the SQL actually issued, without blocking the real call.
    DatasourceOperations spy = Mockito.spy(real);
    doCallRealMethod().when(spy).executeSelect(any(), any());
    TestPersistence tp = newTestPersistence(spy, schemaVersion);
    JdbcBasePersistenceImpl impl = tp.impl();
    PolarisCallContext callCtx = tp.callCtx();

    PolarisBaseEntity e1 = newTestEntity(101L, "e1", 1, 2);
    PolarisBaseEntity e2 = newTestEntity(102L, "e2", 3, 4);
    impl.writeEntity(callCtx, e1, false, null);
    impl.writeEntity(callCtx, e2, false, null);

    PolarisEntityId id1 = new PolarisEntityId(0L, 101L);
    PolarisEntityId missing = new PolarisEntityId(0L, 999L);
    PolarisEntityId id2 = new PolarisEntityId(0L, 102L);
    List<PolarisChangeTrackingVersions> result =
        impl.lookupEntityVersions(callCtx, List.of(id2, missing, id1));

    // Behavioural: order preserved, null for missing.
    assertThat(result).hasSize(3);
    assertThat(result.get(0).entityVersion()).isEqualTo(e2.getEntityVersion());
    assertThat(result.get(0).grantRecordsVersion()).isEqualTo(e2.getGrantRecordsVersion());
    assertThat(result.get(1)).isNull();
    assertThat(result.get(2).entityVersion()).isEqualTo(e1.getEntityVersion());
    assertThat(result.get(2).grantRecordsVersion()).isEqualTo(e1.getGrantRecordsVersion());

    // Perf: the SQL that actually executed must not fetch the large JSON property columns.
    ArgumentCaptor<QueryGenerator.PreparedQuery> captor =
        ArgumentCaptor.forClass(QueryGenerator.PreparedQuery.class);
    verify(spy).executeSelect(captor.capture(), any());
    assertThat(captor.getValue().sql()).doesNotContain("properties");
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 2})
  void hasChildrenUsesExistsQueryNotFullScan(int schemaVersion) throws SQLException, IOException {
    JdbcConnectionPool dataSource =
        JdbcConnectionPool.create(
            "jdbc:h2:mem:has_children_v"
                + schemaVersion
                + "_"
                + System.nanoTime()
                + ";DB_CLOSE_DELAY=-1",
            "sa",
            "");
    DatasourceOperations real = new DatasourceOperations(dataSource, new TestJdbcConfiguration());
    try (InputStream script = DatabaseType.H2.openInitScriptResource(schemaVersion)) {
      real.executeScript(script);
    }
    DatasourceOperations spy = Mockito.spy(real);
    doCallRealMethod().when(spy).executeSelect(any(), any());

    JdbcBasePersistenceImpl impl =
        new JdbcBasePersistenceImpl(
            new PolarisDefaultDiagServiceImpl(),
            spy,
            RANDOM_SECRETS,
            REALM_CONTEXT.getRealmIdentifier(),
            schemaVersion);
    PolarisCallContext callCtx = new PolarisCallContext(REALM_CONTEXT, impl);

    impl.hasChildren(callCtx, null, 0L, 0L);

    ArgumentCaptor<QueryGenerator.PreparedQuery> captor =
        ArgumentCaptor.forClass(QueryGenerator.PreparedQuery.class);
    verify(spy).executeSelect(captor.capture(), any());
    String sql = captor.getValue().sql();
    assertThat(sql).contains("LIMIT 1");
    assertThat(sql).doesNotContain("properties");
    assertThat(sql).doesNotContain("internal_properties");
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 2})
  void lookupEntityGrantRecordsVersionDelegatesToLookupEntityVersions(int schemaVersion)
      throws SQLException, IOException {
    // lookupEntityGrantRecordsVersion delegates to lookupEntityVersions, whose SQL shape (and the
    // exclusion of large property columns) is already covered by the test above; this test only
    // needs to check the delegation's own behavioural contract.
    DatasourceOperations datasourceOperations =
        newH2DatasourceOperations("lookup_grant_version_v", schemaVersion);
    TestPersistence tp = newTestPersistence(datasourceOperations, schemaVersion);
    JdbcBasePersistenceImpl impl = tp.impl();
    PolarisCallContext callCtx = tp.callCtx();

    PolarisBaseEntity e1 = newTestEntity(201L, "e1", 1, 5);
    PolarisBaseEntity e2 = newTestEntity(202L, "e2", 1, 9);
    impl.writeEntity(callCtx, e1, false, null);
    impl.writeEntity(callCtx, e2, false, null);

    // Behavioural: returns the stored grant-records version per entity, 0 for a missing entity.
    assertThat(impl.lookupEntityGrantRecordsVersion(callCtx, 0L, 201L)).isEqualTo(5);
    assertThat(impl.lookupEntityGrantRecordsVersion(callCtx, 0L, 202L)).isEqualTo(9);
    assertThat(impl.lookupEntityGrantRecordsVersion(callCtx, 0L, 999L)).isEqualTo(0);
  }

  /**
   * A mapping is identified by its target and policy ids plus the policy type, not by its
   * parameters. Detach reads the mapping record and then deletes it in a separate round-trip, so a
   * re-attach that rewrites the parameters in between leaves the caller holding a record whose
   * parameters no longer match the stored row. The delete must still remove that row.
   */
  @ParameterizedTest
  @ValueSource(ints = {1, 2, 3, 4, 5})
  void deleteFromPolicyMappingRecordsIgnoresConcurrentParametersUpdate(int schemaVersion)
      throws SQLException, IOException {
    DatasourceOperations datasourceOperations =
        newH2DatasourceOperations("policy_mapping_delete_v", schemaVersion);
    TestPersistence tp = newTestPersistence(datasourceOperations, schemaVersion);
    JdbcBasePersistenceImpl impl = tp.impl();
    PolarisCallContext callCtx = tp.callCtx();

    int policyTypeCode = PredefinedPolicyTypes.DATA_COMPACTION.getCode();
    PolarisPolicyMappingRecord attached =
        newTestPolicyMappingRecord(policyTypeCode, Map.of("version", "1"));
    impl.writeToPolicyMappingRecords(callCtx, attached);

    // A re-attach of the same policy updates the parameters of the existing row in place.
    impl.writeToPolicyMappingRecords(
        callCtx, newTestPolicyMappingRecord(policyTypeCode, Map.of("version", "2")));

    // Guard the premise: the re-attach must really have replaced the stored payload. Were it ever
    // to become a no-op, the stored parameters would still match the stale record below and even a
    // delete that keys on parameters would pass this test.
    PolarisPolicyMappingRecord reattached = lookupTestPolicyMapping(impl, callCtx, policyTypeCode);
    assertThat(reattached).isNotNull();
    assertThat(reattached.getParametersAsMap()).isEqualTo(Map.of("version", "2"));

    // Detach deletes the record it looked up before that update, so its parameters are stale.
    impl.deleteFromPolicyMappingRecords(callCtx, attached);

    assertThat(lookupTestPolicyMapping(impl, callCtx, policyTypeCode)).isNull();
  }

  private static PolarisPolicyMappingRecord lookupTestPolicyMapping(
      JdbcBasePersistenceImpl impl, PolarisCallContext callCtx, int policyTypeCode) {
    return impl.lookupPolicyMappingRecord(
        callCtx,
        POLICY_TARGET_CATALOG_ID,
        POLICY_TARGET_ID,
        policyTypeCode,
        POLICY_CATALOG_ID,
        POLICY_ID);
  }

  private static PolarisPolicyMappingRecord newTestPolicyMappingRecord(
      int policyTypeCode, Map<String, String> parameters) {
    return new PolarisPolicyMappingRecord(
        POLICY_TARGET_CATALOG_ID,
        POLICY_TARGET_ID,
        POLICY_CATALOG_ID,
        POLICY_ID,
        policyTypeCode,
        parameters);
  }

  private static DatasourceOperations newH2DatasourceOperations(
      String dbNamePrefix, int schemaVersion) throws SQLException, IOException {
    JdbcConnectionPool dataSource =
        JdbcConnectionPool.create(
            "jdbc:h2:mem:"
                + dbNamePrefix
                + schemaVersion
                + "_"
                + System.nanoTime()
                + ";DB_CLOSE_DELAY=-1",
            "sa",
            "");
    DatasourceOperations operations =
        new DatasourceOperations(dataSource, new TestJdbcConfiguration());
    try (InputStream script = DatabaseType.H2.openInitScriptResource(schemaVersion)) {
      operations.executeScript(script);
    }
    return operations;
  }

  private static TestPersistence newTestPersistence(
      DatasourceOperations datasourceOperations, int schemaVersion) {
    JdbcBasePersistenceImpl impl =
        new JdbcBasePersistenceImpl(
            new PolarisDefaultDiagServiceImpl(),
            datasourceOperations,
            RANDOM_SECRETS,
            REALM_CONTEXT.getRealmIdentifier(),
            schemaVersion);
    return new TestPersistence(impl, new PolarisCallContext(REALM_CONTEXT, impl));
  }

  private record TestPersistence(JdbcBasePersistenceImpl impl, PolarisCallContext callCtx) {}

  private static PolarisBaseEntity newTestEntity(
      long id, String name, int entityVersion, int grantRecordsVersion) {
    return new PolarisBaseEntity.Builder()
        .id(id)
        .catalogId(0L)
        .parentId(0L)
        .typeCode(PolarisEntityType.PRINCIPAL.getCode())
        .subTypeCode(PolarisEntitySubType.NULL_SUBTYPE.getCode())
        .name(name)
        .entityVersion(entityVersion)
        .grantRecordsVersion(grantRecordsVersion)
        .createTimestamp(System.currentTimeMillis())
        .build();
  }

  /**
   * On the create path (no original entity) {@code writeEntities} must detect an already-created
   * entity with a cheap {@code SELECT 1 ... LIMIT 1} existence check that does not fetch the large
   * JSON property blobs, and a retried create must stay idempotent.
   */
  @Test
  void writeEntitiesCreatePathUsesExistsCheckNotFullRowFetch() throws SQLException, IOException {
    int schemaVersion = 5;
    JdbcConnectionPool dataSource =
        JdbcConnectionPool.create(
            "jdbc:h2:mem:write_entities_create_v"
                + schemaVersion
                + "_"
                + System.nanoTime()
                + ";DB_CLOSE_DELAY=-1",
            "sa",
            "");
    DatasourceOperations real = new DatasourceOperations(dataSource, new TestJdbcConfiguration());
    try (InputStream script = DatabaseType.H2.openInitScriptResource(schemaVersion)) {
      real.executeScript(script);
    }
    DatasourceOperations spy = Mockito.spy(real);

    JdbcBasePersistenceImpl impl =
        new JdbcBasePersistenceImpl(
            new PolarisDefaultDiagServiceImpl(),
            spy,
            RANDOM_SECRETS,
            REALM_CONTEXT.getRealmIdentifier(),
            schemaVersion);
    PolarisCallContext callCtx = new PolarisCallContext(REALM_CONTEXT, impl);

    PolarisBaseEntity entity = principalEntity(1L, "create-me", 1);
    impl.writeEntities(callCtx, List.of(entity), null);

    // The create-path existence check must run as SELECT 1 ... LIMIT 1 (no JSON property columns),
    // on the transaction connection.
    ArgumentCaptor<QueryGenerator.PreparedQuery> captor =
        ArgumentCaptor.forClass(QueryGenerator.PreparedQuery.class);
    verify(spy).executeSelectOverStream(any(Connection.class), captor.capture(), any(), any());
    String sql = captor.getValue().sql();
    assertThat(sql).contains("SELECT 1").contains("LIMIT 1");
    assertThat(sql).doesNotContain("properties");

    // The entity was actually inserted.
    assertThat(
            impl.lookupEntity(callCtx, entity.getCatalogId(), entity.getId(), entity.getTypeCode()))
        .isNotNull();

    // A retried create is idempotent (the existence check short-circuits the insert).
    assertThatCode(() -> impl.writeEntities(callCtx, List.of(entity), null))
        .doesNotThrowAnyException();
  }

  /**
   * On the update path (original entity supplied) {@code writeEntities} already knows the entity
   * exists, so it must not issue any pre-write existence/lookup SELECT before the CAS update.
   */
  @Test
  void writeEntitiesUpdatePathSkipsExistenceLookup() throws SQLException, IOException {
    int schemaVersion = 5;
    JdbcConnectionPool dataSource =
        JdbcConnectionPool.create(
            "jdbc:h2:mem:write_entities_update_v"
                + schemaVersion
                + "_"
                + System.nanoTime()
                + ";DB_CLOSE_DELAY=-1",
            "sa",
            "");
    DatasourceOperations real = new DatasourceOperations(dataSource, new TestJdbcConfiguration());
    try (InputStream script = DatabaseType.H2.openInitScriptResource(schemaVersion)) {
      real.executeScript(script);
    }
    DatasourceOperations spy = Mockito.spy(real);

    JdbcBasePersistenceImpl impl =
        new JdbcBasePersistenceImpl(
            new PolarisDefaultDiagServiceImpl(),
            spy,
            RANDOM_SECRETS,
            REALM_CONTEXT.getRealmIdentifier(),
            schemaVersion);
    PolarisCallContext callCtx = new PolarisCallContext(REALM_CONTEXT, impl);

    PolarisBaseEntity original = principalEntity(1L, "update-me", 1);
    impl.writeEntities(callCtx, List.of(original), null);

    // Only observe the update below.
    Mockito.clearInvocations(spy);

    PolarisBaseEntity updated = principalEntity(1L, "update-me", 2);
    impl.writeEntities(callCtx, List.of(updated), List.of(original));

    // No existence/lookup SELECT is issued before the CAS update.
    verify(spy, never()).executeSelectOverStream(any(Connection.class), any(), any(), any());

    // The CAS update was applied (original v1 -> v2).
    PolarisBaseEntity reloaded =
        impl.lookupEntity(callCtx, updated.getCatalogId(), updated.getId(), updated.getTypeCode());
    assertThat(reloaded).isNotNull();
    assertThat(reloaded.getEntityVersion()).isEqualTo(2);
  }

  private static PolarisBaseEntity principalEntity(long id, String name, int entityVersion) {
    return new PolarisBaseEntity.Builder()
        .id(id)
        .catalogId(0L)
        .parentId(0L)
        .typeCode(PolarisEntityType.PRINCIPAL.getCode())
        .subTypeCode(PolarisEntitySubType.NULL_SUBTYPE.getCode())
        .name(name)
        .entityVersion(entityVersion)
        .grantRecordsVersion(1)
        .createTimestamp(System.currentTimeMillis())
        .build();
  }

  private static final class TestJdbcConfiguration implements RelationalJdbcConfiguration {
    @Override
    public Optional<Integer> maxRetries() {
      return Optional.of(2);
    }

    @Override
    public Optional<Long> maxDurationInMs() {
      return Optional.of(100L);
    }

    @Override
    public Optional<Long> initialDelayInMs() {
      return Optional.of(100L);
    }

    @Override
    public Optional<String> databaseType() {
      return Optional.of("h2");
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 2})
  void listEntitiesBoundsPaginatedQueryByPageSize(int schemaVersion)
      throws SQLException, IOException {
    DatasourceOperations real = newH2DatasourceOperations("list_entities_limit_v", schemaVersion);
    DatasourceOperations spy = Mockito.spy(real);
    doCallRealMethod().when(spy).executeSelectOverStream(any(), any(), any());
    TestPersistence tp = newTestPersistence(spy, schemaVersion);
    JdbcBasePersistenceImpl impl = tp.impl();
    PolarisCallContext callCtx = tp.callCtx();

    for (int i = 0; i < 5; i++) {
      impl.writeEntity(callCtx, newTestEntity(300L + i, "e" + i, 1, 1), false, null);
    }

    Page<EntityNameLookupRecord> page =
        impl.listEntities(
            callCtx,
            0L,
            0L,
            PolarisEntityType.PRINCIPAL,
            PolarisEntitySubType.ANY_SUBTYPE,
            PageToken.fromLimit(2));

    ArgumentCaptor<QueryGenerator.PreparedQuery> captor =
        ArgumentCaptor.forClass(QueryGenerator.PreparedQuery.class);
    verify(spy).executeSelectOverStream(captor.capture(), any(), any());
    // One more than the page size, so the next-page token can still be produced.
    assertThat(captor.getValue().sql()).contains("ORDER BY id ASC").contains("LIMIT 3");

    // The bound must not cost the caller a page or its continuation token.
    assertThat(page.items()).hasSize(2);
    assertThat(page.encodedResponseToken()).isNotNull();
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 2})
  void paginatedListEntitiesWalksEveryPageWhenTotalIsAMultipleOfPageSize(int schemaVersion)
      throws SQLException, IOException {
    DatasourceOperations datasourceOperations =
        newH2DatasourceOperations("list_entities_pages_v", schemaVersion);
    TestPersistence tp = newTestPersistence(datasourceOperations, schemaVersion);
    JdbcBasePersistenceImpl impl = tp.impl();
    PolarisCallContext callCtx = tp.callCtx();

    // An exact multiple of the page size is the case a bound of exactly pageSize would break:
    // the final full page would look like the end of the data and drop the continuation token.
    for (int i = 0; i < 4; i++) {
      impl.writeEntity(callCtx, newTestEntity(500L + i, "e" + i, 1, 1), false, null);
    }

    List<String> seen = new ArrayList<>();
    PageToken pageToken = PageToken.fromLimit(2);
    while (true) {
      Page<EntityNameLookupRecord> page =
          impl.listEntities(
              callCtx,
              0L,
              0L,
              PolarisEntityType.PRINCIPAL,
              PolarisEntitySubType.ANY_SUBTYPE,
              pageToken);
      page.items().forEach(item -> seen.add(item.getName()));
      String next = page.encodedResponseToken();
      if (next == null) {
        break;
      }
      pageToken = PageToken.build(next, 2, () -> true);
    }

    assertThat(seen).containsExactly("e0", "e1", "e2", "e3");
  }

  /** Realm-wide deleteAll must clear tag assignment rows along with the other realm tables. */
  @Test
  void deleteAllClearsTagAssignmentRecords() throws SQLException, IOException {
    int schemaVersion = 7;
    DatasourceOperations datasourceOperations =
        newH2DatasourceOperations("delete_all_tags_v", schemaVersion);
    TestPersistence tp = newTestPersistence(datasourceOperations, schemaVersion);
    JdbcBasePersistenceImpl impl = tp.impl();
    PolarisCallContext callCtx = tp.callCtx();

    impl.writeEntity(callCtx, newTestTagEntity(POLICY_ID, List.of("v1")), true, null);
    impl.writeToTagAssignmentRecords(callCtx, newTestTagAssignmentRecord(POLICY_ID, "v1"));

    impl.deleteAll(callCtx);

    assertThat(
            impl.lookupTagAssignmentRecord(
                callCtx,
                POLICY_TARGET_CATALOG_ID,
                POLICY_TARGET_ID,
                0,
                POLICY_CATALOG_ID,
                POLICY_ID))
        .isNull();
    assertThat(
            impl.loadAllTargetsOnTag(
                callCtx, POLICY_CATALOG_ID, POLICY_ID, null, PageToken.readEverything()))
        .isEmpty();
  }

  /** On schemas without the tag_assignment_record table, deleteAll must skip it and not fail. */
  @ParameterizedTest
  @ValueSource(ints = {1, 2, 3, 4, 5, 6})
  void deleteAllSkipsTagAssignmentsBelowSchemaV7(int schemaVersion)
      throws SQLException, IOException {
    DatasourceOperations datasourceOperations =
        newH2DatasourceOperations("delete_all_gate_v", schemaVersion);
    TestPersistence tp = newTestPersistence(datasourceOperations, schemaVersion);
    JdbcBasePersistenceImpl impl = tp.impl();
    PolarisCallContext callCtx = tp.callCtx();

    impl.writeEntity(callCtx, newTestEntity(600L, "e600", 1, 1), false, null);

    assertThatCode(() -> impl.deleteAll(callCtx)).doesNotThrowAnyException();
  }

  private static TagAssignmentRecord newTestTagAssignmentRecord(long tagId, String value) {
    return new TagAssignmentRecord(
        POLICY_TARGET_CATALOG_ID, POLICY_TARGET_ID, 0, POLICY_CATALOG_ID, tagId, value);
  }

  private static PolarisBaseEntity newTestTagEntity(long tagId, List<String> allowedValues) {
    return new TagEntity.Builder("testTag")
        .setCatalogId(POLICY_CATALOG_ID)
        .setParentId(POLICY_CATALOG_ID)
        .setAllowedValues(allowedValues)
        .setTargetTypes(List.of("catalog", "namespace", "table-like", "column"))
        .setId(tagId)
        .setCreateTimestamp(System.currentTimeMillis())
        .build();
  }

  /**
   * Below schema v7 the tag_assignment_record table does not exist. Writes must fail closed with an
   * error naming the v7 requirement; reads must return empty; the entity-drop cleanup must be a
   * no-op so entity drops keep working.
   */
  @ParameterizedTest
  @ValueSource(ints = {1, 2, 3, 4, 5, 6})
  void tagAssignmentWritesRejectedBelowSchemaV7(int schemaVersion)
      throws SQLException, IOException {
    DatasourceOperations datasourceOperations =
        newH2DatasourceOperations("tag_assignment_gate_v", schemaVersion);
    TestPersistence tp = newTestPersistence(datasourceOperations, schemaVersion);
    JdbcBasePersistenceImpl impl = tp.impl();
    PolarisCallContext callCtx = tp.callCtx();

    PolarisBaseEntity tagEntity = newTestTagEntity(POLICY_ID, List.of("v1"));
    TagAssignmentRecord record = newTestTagAssignmentRecord(POLICY_ID, "v1");

    assertThatThrownBy(() -> impl.writeToTagAssignmentRecords(callCtx, record))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining("schema version");
    assertThatThrownBy(() -> impl.deleteFromTagAssignmentRecords(callCtx, record))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining("schema version");
    assertThatThrownBy(() -> impl.deleteTagAndAllAssignmentRecords(callCtx, tagEntity))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining("schema version");

    // The unassign-path lookup is part of the write path and takes the same gate.
    assertThatThrownBy(
            () ->
                impl.lookupTagAssignmentRecord(
                    callCtx,
                    POLICY_TARGET_CATALOG_ID,
                    POLICY_TARGET_ID,
                    0,
                    POLICY_CATALOG_ID,
                    POLICY_ID))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining("schema version");
    assertThat(
            impl.loadAllTagAssignmentsOnTargetEntity(
                callCtx, POLICY_TARGET_CATALOG_ID, POLICY_TARGET_ID))
        .isEmpty();
    assertThat(
            impl.loadAllTargetsOnTag(
                callCtx, POLICY_CATALOG_ID, POLICY_ID, null, PageToken.readEverything()))
        .isEmpty();

    // entity drops must keep working: cleanup is silently nothing to do
    assertThatCode(
            () ->
                impl.deleteAllEntityTagAssignmentRecords(callCtx, tagEntity, List.of(), List.of()))
        .doesNotThrowAnyException();
  }

  /**
   * The assignment write validates the selected value against the definition's current allowed
   * values inside the same transaction, and re-assigning the same identity replaces the value.
   */
  @Test
  void tagAssignmentWriteValidatesValueAndReplaces() throws SQLException, IOException {
    int schemaVersion = 7;
    DatasourceOperations datasourceOperations =
        newH2DatasourceOperations("tag_assignment_write_v", schemaVersion);
    TestPersistence tp = newTestPersistence(datasourceOperations, schemaVersion);
    JdbcBasePersistenceImpl impl = tp.impl();
    PolarisCallContext callCtx = tp.callCtx();

    impl.writeEntity(callCtx, newTestTagEntity(POLICY_ID, List.of("v1", "v2")), true, null);

    impl.writeToTagAssignmentRecords(callCtx, newTestTagAssignmentRecord(POLICY_ID, "v1"));
    impl.writeToTagAssignmentRecords(callCtx, newTestTagAssignmentRecord(POLICY_ID, "v2"));
    TagAssignmentRecord stored =
        impl.lookupTagAssignmentRecord(
            callCtx, POLICY_TARGET_CATALOG_ID, POLICY_TARGET_ID, 0, POLICY_CATALOG_ID, POLICY_ID);
    assertThat(stored).isNotNull();
    assertThat(stored.getValue()).isEqualTo("v2");

    assertThatThrownBy(
            () ->
                impl.writeToTagAssignmentRecords(
                    callCtx, newTestTagAssignmentRecord(POLICY_ID, "nope")))
        .isInstanceOf(BadRequestException.class);

    // a definition that does not resolve inside the transaction rejects the write
    assertThatThrownBy(
            () ->
                impl.writeToTagAssignmentRecords(
                    callCtx, newTestTagAssignmentRecord(POLICY_ID + 1, "v1")))
        .isInstanceOf(NoSuchTagException.class);
  }

  /**
   * The delete reports whether it actually removed a row: a first delete of an existing assignment
   * answers true, and deleting the same identity again (nothing left to remove) answers false. This
   * is the signal the unassign manager path relies on to tell its own delete apart from one a
   * concurrent unassign already performed.
   */
  @Test
  void deleteFromTagAssignmentRecordsReportsWhetherARowWasRemoved()
      throws SQLException, IOException {
    int schemaVersion = 7;
    DatasourceOperations datasourceOperations =
        newH2DatasourceOperations("tag_assignment_delete_v", schemaVersion);
    TestPersistence tp = newTestPersistence(datasourceOperations, schemaVersion);
    JdbcBasePersistenceImpl impl = tp.impl();
    PolarisCallContext callCtx = tp.callCtx();

    impl.writeEntity(callCtx, newTestTagEntity(POLICY_ID, List.of("v1")), true, null);
    TagAssignmentRecord record = newTestTagAssignmentRecord(POLICY_ID, "v1");
    impl.writeToTagAssignmentRecords(callCtx, record);

    assertThat(impl.deleteFromTagAssignmentRecords(callCtx, record)).isTrue();
    assertThat(impl.deleteFromTagAssignmentRecords(callCtx, record)).isFalse();
  }

  /**
   * A single conflict at the tag write's in-transaction re-read of the tag definition (the seam
   * that, on Postgres, must not retry itself on the same aborted transaction connection; see {@link
   * DatasourceOperationsTest#executeSelectNoRetryDoesNotRetryOnSameConnection} and {@link
   * DatasourceOperationsTest#executeSelectWithConnectionRetriesOnSameConnectionAndCanSurfaceAbortedTransaction}
   * for that seam in isolation) must not fail the write outright: it surfaces at the transaction
   * boundary, where the write's own conflict re-run already re-tries the whole transaction on a
   * fresh connection.
   */
  @Test
  void tagWriteSurvivesOneConflictAtInTransactionSelect() throws SQLException, IOException {
    int schemaVersion = 7;
    DatasourceOperations real =
        newH2DatasourceOperations("tag_write_select_conflict_v", schemaVersion);
    DatasourceOperations spy = Mockito.spy(real);
    TestPersistence tp = newTestPersistence(spy, schemaVersion);
    JdbcBasePersistenceImpl impl = tp.impl();
    PolarisCallContext callCtx = tp.callCtx();
    impl.writeEntity(callCtx, newTestTagEntity(POLICY_ID, List.of("v1", "v2")), true, null);

    AtomicInteger tagEntitySelects = new AtomicInteger();
    Mockito.doAnswer(
            invocation -> {
              QueryGenerator.PreparedQuery q = invocation.getArgument(1);
              if (q.sql().contains(ModelEntity.TABLE_NAME)
                  && tagEntitySelects.getAndIncrement() == 0) {
                throw new SQLException("injected serialization failure", "40001");
              }
              return invocation.callRealMethod();
            })
        .when(spy)
        .executeSelectNoRetry(Mockito.any(Connection.class), Mockito.any(), Mockito.any());

    impl.writeToTagAssignmentRecords(callCtx, newTestTagAssignmentRecord(POLICY_ID, "v2"));

    // confirm the injected fault actually fired, so a passing test is not merely vacuous
    assertThat(tagEntitySelects.get()).isGreaterThanOrEqualTo(2);
    TagAssignmentRecord stored =
        impl.lookupTagAssignmentRecord(
            callCtx, POLICY_TARGET_CATALOG_ID, POLICY_TARGET_ID, 0, POLICY_CATALOG_ID, POLICY_ID);
    assertThat(stored).isNotNull();
    assertThat(stored.getValue()).isEqualTo("v2");
  }

  /**
   * A configuration with no retry settings at all: the datasource budget is the 1-attempt default.
   */
  private static final class DefaultJdbcConfiguration implements RelationalJdbcConfiguration {
    @Override
    public Optional<Integer> maxRetries() {
      return Optional.empty();
    }

    @Override
    public Optional<Long> maxDurationInMs() {
      return Optional.empty();
    }

    @Override
    public Optional<Long> initialDelayInMs() {
      return Optional.empty();
    }

    @Override
    public Optional<String> databaseType() {
      return Optional.of("h2");
    }
  }

  private static DatasourceOperations newDefaultConfigH2(String dbNamePrefix, int schemaVersion)
      throws SQLException, IOException {
    JdbcConnectionPool dataSource =
        JdbcConnectionPool.create(
            "jdbc:h2:mem:"
                + dbNamePrefix
                + schemaVersion
                + "_"
                + System.nanoTime()
                + ";DB_CLOSE_DELAY=-1",
            "sa",
            "");
    DatasourceOperations operations =
        new DatasourceOperations(dataSource, new DefaultJdbcConfiguration());
    try (InputStream script = DatabaseType.H2.openInitScriptResource(schemaVersion)) {
      operations.executeScript(script);
    }
    return operations;
  }

  /**
   * The assignment write owns its conflict re-run: under the datasource's default 1-attempt budget,
   * one injected conflict at the tag write (unique violation under READ COMMITTED, or a
   * serialization failure under SERIALIZABLE) is re-run once and lands on the replaced value.
   */
  @ParameterizedTest
  @ValueSource(strings = {"23505", "40001"})
  void tagAssignmentWriteRerunsOnceOnConflictUnderDefaultBudget(String sqlState)
      throws SQLException, IOException {
    int schemaVersion = 7;
    DatasourceOperations real = newDefaultConfigH2("tag_assignment_conflict_v", schemaVersion);
    DatasourceOperations spy = Mockito.spy(real);
    TestPersistence tp = newTestPersistence(spy, schemaVersion);
    JdbcBasePersistenceImpl impl = tp.impl();
    PolarisCallContext callCtx = tp.callCtx();
    impl.writeEntity(callCtx, newTestTagEntity(POLICY_ID, List.of("v1", "v2")), true, null);

    // First attempt's INSERT fails with the conflict; the write re-runs and (no winner row in this
    // variant) inserts cleanly on the second attempt.
    AtomicInteger inserts = new AtomicInteger();
    Mockito.doAnswer(
            invocation -> {
              QueryGenerator.PreparedQuery q = invocation.getArgument(1);
              if (q.sql().startsWith("INSERT INTO ")
                  && q.sql().contains(ModelTagAssignmentRecord.TABLE_NAME)
                  && inserts.getAndIncrement() == 0) {
                throw new SQLException("injected conflict", sqlState);
              }
              return invocation.callRealMethod();
            })
        .when(spy)
        .execute(Mockito.any(java.sql.Connection.class), Mockito.any());

    impl.writeToTagAssignmentRecords(callCtx, newTestTagAssignmentRecord(POLICY_ID, "v2"));

    TagAssignmentRecord stored =
        impl.lookupTagAssignmentRecord(
            callCtx, POLICY_TARGET_CATALOG_ID, POLICY_TARGET_ID, 0, POLICY_CATALOG_ID, POLICY_ID);
    assertThat(stored).isNotNull();
    assertThat(stored.getValue()).isEqualTo("v2");
    assertThat(inserts.get()).isEqualTo(2);
  }

  /** A second conflict on the re-run surfaces unchanged: the local re-run is exactly one. */
  @Test
  void tagAssignmentWriteSecondConflictSurfaces() throws SQLException, IOException {
    int schemaVersion = 7;
    DatasourceOperations real = newDefaultConfigH2("tag_assignment_conflict2_v", schemaVersion);
    DatasourceOperations spy = Mockito.spy(real);
    TestPersistence tp = newTestPersistence(spy, schemaVersion);
    JdbcBasePersistenceImpl impl = tp.impl();
    PolarisCallContext callCtx = tp.callCtx();
    impl.writeEntity(callCtx, newTestTagEntity(POLICY_ID, List.of("v1")), true, null);
    Mockito.doAnswer(
            invocation -> {
              QueryGenerator.PreparedQuery q = invocation.getArgument(1);
              if (q.sql().startsWith("INSERT INTO ")
                  && q.sql().contains(ModelTagAssignmentRecord.TABLE_NAME)) {
                throw new SQLException("injected conflict", "23505");
              }
              return invocation.callRealMethod();
            })
        .when(spy)
        .execute(Mockito.any(java.sql.Connection.class), Mockito.any());

    assertThatThrownBy(
            () ->
                impl.writeToTagAssignmentRecords(
                    callCtx, newTestTagAssignmentRecord(POLICY_ID, "v1")))
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining("Failed to write to tag assignment records");
  }

  /**
   * The local re-run belongs to the tag write only: a conflict raised by an unrelated write on the
   * same default-budget datasource is not re-run beyond that budget.
   */
  @Test
  void conflictOutsideTagWriteIsNotRerunBeyondGlobalBudget() throws SQLException, IOException {
    int schemaVersion = 7;
    DatasourceOperations real = newDefaultConfigH2("tag_assignment_other_v", schemaVersion);
    DatasourceOperations spy = Mockito.spy(real);
    TestPersistence tp = newTestPersistence(spy, schemaVersion);
    JdbcBasePersistenceImpl impl = tp.impl();
    PolarisCallContext callCtx = tp.callCtx();
    AtomicInteger grantWrites = new AtomicInteger();
    Mockito.doAnswer(
            invocation -> {
              grantWrites.incrementAndGet();
              throw new SQLException("injected conflict", "40001");
            })
        .when(spy)
        .executeUpdate(Mockito.any(QueryGenerator.PreparedQuery.class));

    PolarisGrantRecord grant =
        new PolarisGrantRecord(
            SECURABLE_CATALOG_ID, SECURABLE_ID, GRANTEE_CATALOG_ID, GRANTEE_ID, PRIVILEGE_CODE);
    assertThatThrownBy(() -> impl.writeToGrantRecords(callCtx, grant))
        .isInstanceOf(RuntimeException.class);
    assertThat(grantWrites.get()).isEqualTo(1);
  }

  /**
   * Two first-time writers of one identity can both miss the existing-row check and both insert;
   * the loser's unique violation reruns its whole transaction once, which then finds the winner's
   * row and replaces the value. Simulated by inserting the winner's row from inside the loser's
   * first attempt.
   */
  @Test
  void tagAssignmentWriteRerunsOnceOnUniqueViolation() throws SQLException, IOException {
    int schemaVersion = 7;
    DatasourceOperations real = newH2DatasourceOperations("tag_assignment_race_v", schemaVersion);
    DatasourceOperations spy = Mockito.spy(real);
    TestPersistence tp = newTestPersistence(spy, schemaVersion);
    JdbcBasePersistenceImpl impl = tp.impl();
    PolarisCallContext callCtx = tp.callCtx();
    impl.writeEntity(callCtx, newTestTagEntity(POLICY_ID, List.of("v1", "v2")), true, null);

    // First attempt: after the loser's existing-row SELECT ran empty, the winner commits "v1";
    // the loser's INSERT then fails with 23505 and the whole callback reruns.
    AtomicInteger inserts = new AtomicInteger();
    Mockito.doAnswer(
            invocation -> {
              QueryGenerator.PreparedQuery q = invocation.getArgument(1);
              if (q.sql().startsWith("INSERT INTO ")
                  && q.sql().contains(ModelTagAssignmentRecord.TABLE_NAME)
                  && inserts.getAndIncrement() == 0) {
                real.runWithinTransaction(
                    winner -> {
                      real.execute(winner, q);
                      return true;
                    });
              }
              return invocation.callRealMethod();
            })
        .when(spy)
        .execute(Mockito.any(java.sql.Connection.class), Mockito.any());

    impl.writeToTagAssignmentRecords(callCtx, newTestTagAssignmentRecord(POLICY_ID, "v2"));

    TagAssignmentRecord stored =
        impl.lookupTagAssignmentRecord(
            callCtx, POLICY_TARGET_CATALOG_ID, POLICY_TARGET_ID, 0, POLICY_CATALOG_ID, POLICY_ID);
    assertThat(stored).isNotNull();
    assertThat(stored.getValue()).isEqualTo("v2");
    assertThat(inserts.get()).isEqualTo(1);
  }

  /**
   * The write re-reads the tag definition inside its own transaction, but that read alone does not
   * stop a concurrent update of the definition's allowed values from committing before this write
   * reaches its own version check: the version check below the read now closes exactly that window.
   * When the update commits first, the write's version check finds the definition moved, re-runs
   * its whole transaction, re-reads the new allowed values, and rejects a value the update just
   * removed.
   */
  @Test
  void tagAssignmentWriteRejectsValueRemovedByConcurrentDefinitionUpdate()
      throws SQLException, IOException {
    int schemaVersion = 7;
    DatasourceOperations real =
        newH2DatasourceOperations("tag_assignment_concurrent_update_v", schemaVersion);
    DatasourceOperations spy = Mockito.spy(real);
    TestPersistence tp = newTestPersistence(spy, schemaVersion);
    JdbcBasePersistenceImpl impl = tp.impl();
    PolarisCallContext callCtx = tp.callCtx();

    PolarisBaseEntity original = newTestTagEntity(POLICY_ID, List.of("v1", "v2"));
    impl.writeEntity(callCtx, original, true, null);

    AtomicBoolean definitionUpdateApplied = new AtomicBoolean(false);

    // Right after the write's own read of the definition (still the old allowed-values list, "v1"
    // included, on this open transaction) but before its version check runs, commit a concurrent
    // update of the definition to a new list that drops "v1", on a separate, genuinely
    // independent transaction. The update's own CAS commits before this write's version check
    // ever runs, so the update-first ordering is forced deterministically without needing two
    // threads; each write here is still a real, independently committed unit of work.
    Mockito.doAnswer(
            invocation -> {
              Object selected = invocation.callRealMethod();
              QueryGenerator.PreparedQuery q = invocation.getArgument(1);
              if (q.sql().contains(ModelEntity.TABLE_NAME)
                  && definitionUpdateApplied.compareAndSet(false, true)) {
                TagEntity currentTag = TagEntity.of(original);
                TagEntity updatedTag =
                    new TagEntity.Builder(currentTag).setAllowedValues(List.of("v2")).build();
                PolarisBaseEntity updatedEntity =
                    new PolarisBaseEntity.Builder(updatedTag).entityVersion(2).build();
                impl.writeEntity(callCtx, updatedEntity, false, original);
              }
              return selected;
            })
        .when(spy)
        .executeSelectNoRetry(Mockito.any(Connection.class), Mockito.any(), Mockito.any());

    assertThatThrownBy(
            () ->
                impl.writeToTagAssignmentRecords(
                    callCtx, newTestTagAssignmentRecord(POLICY_ID, "v1")))
        .isInstanceOf(BadRequestException.class);

    // confirm the injected update actually fired, so a passing test is not merely vacuous
    assertThat(definitionUpdateApplied.get()).isTrue();
    assertThat(
            impl.lookupTagAssignmentRecord(
                callCtx,
                POLICY_TARGET_CATALOG_ID,
                POLICY_TARGET_ID,
                0,
                POLICY_CATALOG_ID,
                POLICY_ID))
        .isNull();
    PolarisBaseEntity reloadedTag =
        impl.lookupEntity(
            callCtx, original.getCatalogId(), original.getId(), original.getTypeCode());
    assertThat(TagEntity.of(reloadedTag).getAllowedValues()).containsExactly("v2");
  }

  /**
   * The mirror ordering: this write reaches its version check first and takes the definition row's
   * write lock before a concurrent update of the definition can. The update must block behind that
   * lock rather than run through and commit underneath the write, and once the write commits
   * (grandfathering the now-removed value "v1"), the update proceeds and applies on top.
   */
  @Test
  void tagAssignmentWriteHoldsDefinitionLockUntilItsOwnCommit() throws Exception {
    int schemaVersion = 7;
    DatasourceOperations real =
        newH2DatasourceOperations("tag_assignment_lock_order_v", schemaVersion);
    DatasourceOperations spy = Mockito.spy(real);
    TestPersistence tp = newTestPersistence(spy, schemaVersion);
    JdbcBasePersistenceImpl impl = tp.impl();
    PolarisCallContext callCtx = tp.callCtx();

    PolarisBaseEntity original = newTestTagEntity(POLICY_ID, List.of("v1", "v2"));
    impl.writeEntity(callCtx, original, true, null);

    CountDownLatch versionCheckTaken = new CountDownLatch(1);
    CountDownLatch releaseWrite = new CountDownLatch(1);
    Mockito.doAnswer(
            invocation -> {
              Integer rowsUpdated = (Integer) invocation.callRealMethod();
              QueryGenerator.PreparedQuery q = invocation.getArgument(1);
              if (q.sql().startsWith("UPDATE ")
                  && q.sql().contains(ModelEntity.TABLE_NAME)
                  && q.sql().contains("entity_version = entity_version")) {
                // The lock is already taken above (the real update already ran); hold this
                // transaction open here so the test can observe the concurrent update blocking
                // on it before this write is allowed to proceed to its own commit.
                versionCheckTaken.countDown();
                if (!releaseWrite.await(5, TimeUnit.SECONDS)) {
                  throw new AssertionError("test did not release the paused write in time");
                }
              }
              return rowsUpdated;
            })
        .when(spy)
        .execute(Mockito.any(Connection.class), Mockito.any());

    ExecutorService pool = Executors.newFixedThreadPool(2);
    try {
      Future<?> writeAttempt =
          pool.submit(
              () ->
                  impl.writeToTagAssignmentRecords(
                      callCtx, newTestTagAssignmentRecord(POLICY_ID, "v1")));

      assertThat(versionCheckTaken.await(5, TimeUnit.SECONDS)).isTrue();

      TagEntity currentTag = TagEntity.of(original);
      TagEntity updatedTag =
          new TagEntity.Builder(currentTag).setAllowedValues(List.of("v2")).build();
      PolarisBaseEntity updatedEntity =
          new PolarisBaseEntity.Builder(updatedTag).entityVersion(2).build();
      Future<?> updateAttempt =
          pool.submit(() -> impl.writeEntity(callCtx, updatedEntity, false, original));

      // The concurrent update must still be blocked behind the write's held lock: it must not
      // complete while the write is paused.
      assertThatThrownBy(() -> updateAttempt.get(200, TimeUnit.MILLISECONDS))
          .isInstanceOf(TimeoutException.class);

      releaseWrite.countDown();
      writeAttempt.get(5, TimeUnit.SECONDS);
      updateAttempt.get(5, TimeUnit.SECONDS);
    } finally {
      pool.shutdownNow();
    }

    // the assignment of the now-superseded value is grandfathered ...
    TagAssignmentRecord stored =
        impl.lookupTagAssignmentRecord(
            callCtx, POLICY_TARGET_CATALOG_ID, POLICY_TARGET_ID, 0, POLICY_CATALOG_ID, POLICY_ID);
    assertThat(stored).isNotNull();
    assertThat(stored.getValue()).isEqualTo("v1");
    // ... and the update that was blocked behind it still applies once the write commits.
    PolarisBaseEntity reloadedTag =
        impl.lookupEntity(
            callCtx, original.getCatalogId(), original.getId(), original.getTypeCode());
    assertThat(TagEntity.of(reloadedTag).getAllowedValues()).containsExactly("v2");
  }

  /**
   * The write's own re-run belongs to a definition-version conflict too, not only to a same-
   * identity conflict on the assignment row: a single injected conflict at the version check is
   * re-run once and lands on the assigned value.
   */
  @Test
  void tagAssignmentWriteRerunsOnceOnDefinitionVersionConflict() throws SQLException, IOException {
    int schemaVersion = 7;
    DatasourceOperations real =
        newDefaultConfigH2("tag_assignment_version_conflict_v", schemaVersion);
    DatasourceOperations spy = Mockito.spy(real);
    TestPersistence tp = newTestPersistence(spy, schemaVersion);
    JdbcBasePersistenceImpl impl = tp.impl();
    PolarisCallContext callCtx = tp.callCtx();
    impl.writeEntity(callCtx, newTestTagEntity(POLICY_ID, List.of("v1", "v2")), true, null);

    AtomicInteger versionChecks = new AtomicInteger();
    Mockito.doAnswer(
            invocation -> {
              QueryGenerator.PreparedQuery q = invocation.getArgument(1);
              if (q.sql().startsWith("UPDATE ")
                  && q.sql().contains(ModelEntity.TABLE_NAME)
                  && q.sql().contains("entity_version = entity_version")
                  && versionChecks.getAndIncrement() == 0) {
                throw new JdbcBasePersistenceImpl.DefinitionVersionConflictException(
                    "injected definition version conflict");
              }
              return invocation.callRealMethod();
            })
        .when(spy)
        .execute(Mockito.any(java.sql.Connection.class), Mockito.any());

    impl.writeToTagAssignmentRecords(callCtx, newTestTagAssignmentRecord(POLICY_ID, "v2"));

    TagAssignmentRecord stored =
        impl.lookupTagAssignmentRecord(
            callCtx, POLICY_TARGET_CATALOG_ID, POLICY_TARGET_ID, 0, POLICY_CATALOG_ID, POLICY_ID);
    assertThat(stored).isNotNull();
    assertThat(stored.getValue()).isEqualTo("v2");
    assertThat(versionChecks.get()).isEqualTo(2);
  }

  /** A second definition-version conflict on the re-run surfaces unchanged: the bound is 2. */
  @Test
  void tagAssignmentWriteSecondDefinitionVersionConflictSurfaces()
      throws SQLException, IOException {
    int schemaVersion = 7;
    DatasourceOperations real =
        newDefaultConfigH2("tag_assignment_version_conflict2_v", schemaVersion);
    DatasourceOperations spy = Mockito.spy(real);
    TestPersistence tp = newTestPersistence(spy, schemaVersion);
    JdbcBasePersistenceImpl impl = tp.impl();
    PolarisCallContext callCtx = tp.callCtx();
    impl.writeEntity(callCtx, newTestTagEntity(POLICY_ID, List.of("v1")), true, null);

    AtomicInteger versionChecks = new AtomicInteger();
    Mockito.doAnswer(
            invocation -> {
              QueryGenerator.PreparedQuery q = invocation.getArgument(1);
              if (q.sql().startsWith("UPDATE ")
                  && q.sql().contains(ModelEntity.TABLE_NAME)
                  && q.sql().contains("entity_version = entity_version")) {
                versionChecks.incrementAndGet();
                throw new JdbcBasePersistenceImpl.DefinitionVersionConflictException(
                    "injected definition version conflict");
              }
              return invocation.callRealMethod();
            })
        .when(spy)
        .execute(Mockito.any(java.sql.Connection.class), Mockito.any());

    assertThatThrownBy(
            () ->
                impl.writeToTagAssignmentRecords(
                    callCtx, newTestTagAssignmentRecord(POLICY_ID, "v1")))
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining("Failed to write to tag assignment records");
    assertThat(versionChecks.get()).isEqualTo(2);
  }

  /**
   * The delete is keyed on the assignment identity, not the stored value, so a concurrent value
   * replacement cannot turn the delete into a no-op.
   */
  @Test
  void deleteFromTagAssignmentRecordsIgnoresConcurrentValueUpdate()
      throws SQLException, IOException {
    int schemaVersion = 7;
    DatasourceOperations datasourceOperations =
        newH2DatasourceOperations("tag_assignment_delete_v", schemaVersion);
    TestPersistence tp = newTestPersistence(datasourceOperations, schemaVersion);
    JdbcBasePersistenceImpl impl = tp.impl();
    PolarisCallContext callCtx = tp.callCtx();

    impl.writeEntity(callCtx, newTestTagEntity(POLICY_ID, List.of("v1", "v2")), true, null);

    TagAssignmentRecord assigned = newTestTagAssignmentRecord(POLICY_ID, "v1");
    impl.writeToTagAssignmentRecords(callCtx, assigned);
    impl.writeToTagAssignmentRecords(callCtx, newTestTagAssignmentRecord(POLICY_ID, "v2"));

    impl.deleteFromTagAssignmentRecords(callCtx, assigned);
    assertThat(
            impl.lookupTagAssignmentRecord(
                callCtx,
                POLICY_TARGET_CATALOG_ID,
                POLICY_TARGET_ID,
                0,
                POLICY_CATALOG_ID,
                POLICY_ID))
        .isNull();
  }

  /**
   * The combined delete of the definition entity and its assignment rows commits together or not at
   * all. A failure injected between the two deletes must roll back the entity delete too; a
   * success-path test alone cannot distinguish genuinely atomic from happened-to-complete.
   */
  @Test
  void deleteTagAndAllAssignmentRecordsRollsBackOnFailure() throws SQLException, IOException {
    int schemaVersion = 7;
    DatasourceOperations datasourceOperations =
        newH2DatasourceOperations("tag_assignment_atomic_v", schemaVersion);
    TestPersistence tp = newTestPersistence(datasourceOperations, schemaVersion);
    JdbcBasePersistenceImpl impl = tp.impl();
    PolarisCallContext callCtx = tp.callCtx();

    PolarisBaseEntity tagEntity = newTestTagEntity(POLICY_ID, List.of("v1"));
    impl.writeEntity(callCtx, tagEntity, true, null);
    impl.writeToTagAssignmentRecords(callCtx, newTestTagAssignmentRecord(POLICY_ID, "v1"));

    // an impl whose datasource fails the assignment delete AFTER the entity delete executed
    DatasourceOperations failingOperations = Mockito.spy(datasourceOperations);
    Mockito.doAnswer(
            invocation -> {
              QueryGenerator.PreparedQuery query = invocation.getArgument(1);
              if (query.sql().startsWith("DELETE")
                  && query.sql().contains(ModelTagAssignmentRecord.TABLE_NAME)) {
                throw new SQLException("injected failure between the two deletes");
              }
              return invocation.callRealMethod();
            })
        .when(failingOperations)
        .execute(Mockito.any(Connection.class), Mockito.any());
    JdbcBasePersistenceImpl failingImpl =
        new JdbcBasePersistenceImpl(
            new PolarisDefaultDiagServiceImpl(),
            failingOperations,
            RANDOM_SECRETS,
            REALM_CONTEXT.getRealmIdentifier(),
            schemaVersion);

    assertThatThrownBy(() -> failingImpl.deleteTagAndAllAssignmentRecords(callCtx, tagEntity))
        .isInstanceOf(RuntimeException.class);

    // full rollback: the definition row and the assignment row are both still present
    assertThat(
            impl.lookupEntity(
                callCtx, tagEntity.getCatalogId(), tagEntity.getId(), tagEntity.getTypeCode()))
        .isNotNull();
    assertThat(
            impl.lookupTagAssignmentRecord(
                callCtx,
                POLICY_TARGET_CATALOG_ID,
                POLICY_TARGET_ID,
                0,
                POLICY_CATALOG_ID,
                POLICY_ID))
        .isNotNull();

    // and the unbroken path removes both together
    impl.deleteTagAndAllAssignmentRecords(callCtx, tagEntity);
    assertThat(
            impl.lookupEntity(
                callCtx, tagEntity.getCatalogId(), tagEntity.getId(), tagEntity.getTypeCode()))
        .isNull();
    assertThat(
            impl.lookupTagAssignmentRecord(
                callCtx,
                POLICY_TARGET_CATALOG_ID,
                POLICY_TARGET_ID,
                0,
                POLICY_CATALOG_ID,
                POLICY_ID))
        .isNull();
  }

  /**
   * Doc-required fault injection for the OTHER side of the contract: tag-assignment cleanup during
   * a target entity drop is best-effort. A real persistence failure inside the cleanup must not
   * fail the target drop; the leftover assignment row stays behind (orphaned, hidden from reads).
   */
  @Test
  void targetDropSucceedsWhenTagCleanupFails() throws SQLException, IOException {
    int schemaVersion = 7;
    DatasourceOperations datasourceOperations =
        newH2DatasourceOperations("tag_cleanup_best_effort_v", schemaVersion);
    TestPersistence tp = newTestPersistence(datasourceOperations, schemaVersion);
    JdbcBasePersistenceImpl impl = tp.impl();
    PolarisCallContext callCtx = tp.callCtx();

    PolarisBaseEntity target =
        new PolarisBaseEntity.Builder()
            .id(POLICY_TARGET_ID)
            .catalogId(POLICY_TARGET_CATALOG_ID)
            .parentId(POLICY_TARGET_CATALOG_ID)
            .typeCode(PolarisEntityType.TABLE_LIKE.getCode())
            .subTypeCode(PolarisEntitySubType.ICEBERG_TABLE.getCode())
            .name("cleanup_target")
            .createTimestamp(System.currentTimeMillis())
            .build();
    impl.writeEntity(callCtx, target, true, null);
    impl.writeEntity(callCtx, newTestTagEntity(POLICY_ID, List.of("v1")), true, null);
    impl.writeToTagAssignmentRecords(callCtx, newTestTagAssignmentRecord(POLICY_ID, "v1"));

    // fail exactly the seam the drop-path cleanup reads through
    JdbcBasePersistenceImpl failingImpl = Mockito.spy(impl);
    Mockito.doThrow(new RuntimeException("injected cleanup failure"))
        .when(failingImpl)
        .loadAllTagAssignmentsOnTargetEntity(
            Mockito.any(), Mockito.eq(POLICY_TARGET_CATALOG_ID), Mockito.eq(POLICY_TARGET_ID));
    PolarisCallContext failingCallCtx = new PolarisCallContext(REALM_CONTEXT, failingImpl);
    AtomicOperationMetaStoreManager metaStoreManager =
        new AtomicOperationMetaStoreManager(Clock.systemUTC(), new PolarisDefaultDiagServiceImpl());

    var dropped =
        metaStoreManager.dropEntityIfExists(failingCallCtx, null, target, Map.of(), false);

    // the drop itself succeeded even though the cleanup blew up
    assertThat(dropped.isSuccess()).isTrue();
    assertThat(
            impl.lookupEntity(callCtx, target.getCatalogId(), target.getId(), target.getTypeCode()))
        .isNull();
    // the assignment row is left behind: cleanup really did fail rather than run
    assertThat(
            impl.lookupTagAssignmentRecord(
                callCtx,
                POLICY_TARGET_CATALOG_ID,
                POLICY_TARGET_ID,
                0,
                POLICY_CATALOG_ID,
                POLICY_ID))
        .isNotNull();
  }

  /**
   * The atomic manager's unassign path looks up the assignment and then deletes it as two separate
   * persistence calls, not one atomic step. A concurrent unassign that wins the race in between
   * must make this call's own delete remove nothing; the manager must then report
   * TAG_ASSIGNMENT_NOT_FOUND rather than trusting the (by-then-stale) lookup result. Simulated by
   * deleting the row, as the concurrent winner would, right after this call's own lookup observed
   * it present but before its delete runs.
   */
  @Test
  void unassignRaceAfterLookupReportsNotFound() throws SQLException, IOException {
    int schemaVersion = 7;
    DatasourceOperations datasourceOperations =
        newH2DatasourceOperations("tag_unassign_race_v", schemaVersion);
    TestPersistence tp = newTestPersistence(datasourceOperations, schemaVersion);
    JdbcBasePersistenceImpl impl = tp.impl();
    PolarisCallContext callCtx = tp.callCtx();

    PolarisBaseEntity target =
        new PolarisBaseEntity.Builder()
            .id(POLICY_TARGET_ID)
            .catalogId(POLICY_TARGET_CATALOG_ID)
            .parentId(POLICY_TARGET_CATALOG_ID)
            .typeCode(PolarisEntityType.TABLE_LIKE.getCode())
            .subTypeCode(PolarisEntitySubType.ICEBERG_TABLE.getCode())
            .name("race_target")
            .createTimestamp(System.currentTimeMillis())
            .build();
    impl.writeEntity(callCtx, target, true, null);
    PolarisBaseEntity tagEntity = newTestTagEntity(POLICY_ID, List.of("v1"));
    impl.writeEntity(callCtx, tagEntity, true, null);
    TagEntity tag = TagEntity.of(tagEntity);
    TagAssignmentRecord record = newTestTagAssignmentRecord(POLICY_ID, "v1");
    impl.writeToTagAssignmentRecords(callCtx, record);

    JdbcBasePersistenceImpl spyImpl = Mockito.spy(impl);
    Mockito.doAnswer(
            invocation -> {
              // The manager's own lookup still observes the row (the real call below runs
              // first); a concurrent winner then removes it before the manager's delete runs.
              TagAssignmentRecord found = (TagAssignmentRecord) invocation.callRealMethod();
              impl.deleteFromTagAssignmentRecords(callCtx, record);
              return found;
            })
        .when(spyImpl)
        .lookupTagAssignmentRecord(
            Mockito.any(),
            Mockito.eq(POLICY_TARGET_CATALOG_ID),
            Mockito.eq(POLICY_TARGET_ID),
            Mockito.eq(0),
            Mockito.eq(POLICY_CATALOG_ID),
            Mockito.eq(POLICY_ID));
    PolarisCallContext raceCallCtx = new PolarisCallContext(REALM_CONTEXT, spyImpl);
    AtomicOperationMetaStoreManager metaStoreManager =
        new AtomicOperationMetaStoreManager(Clock.systemUTC(), new PolarisDefaultDiagServiceImpl());

    TagAssignmentResult result =
        metaStoreManager.unassignTagFromEntity(raceCallCtx, List.of(), target, 0, List.of(), tag);

    assertThat(result.getReturnStatus())
        .isEqualTo(BaseResult.ReturnStatus.TAG_ASSIGNMENT_NOT_FOUND);
    // this call's own delete really did run and really did find nothing, rather than the not-
    // found status coming from some other cause
    assertThat(
            impl.lookupTagAssignmentRecord(
                callCtx,
                POLICY_TARGET_CATALOG_ID,
                POLICY_TARGET_ID,
                0,
                POLICY_CATALOG_ID,
                POLICY_ID))
        .isNull();
  }
}
