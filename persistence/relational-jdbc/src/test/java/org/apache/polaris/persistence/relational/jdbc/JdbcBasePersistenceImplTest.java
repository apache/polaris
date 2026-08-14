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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
import org.apache.polaris.core.persistence.EntityAlreadyExistsException;
import org.apache.polaris.core.persistence.RetryOnConcurrencyException;
import org.apache.polaris.core.persistence.pagination.Page;
import org.apache.polaris.core.persistence.pagination.PageToken;
import org.apache.polaris.core.policy.PolarisPolicyMappingRecord;
import org.apache.polaris.core.policy.PredefinedPolicyTypes;
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

    // Detach deletes the record it looked up before that update, so its parameters are stale.
    impl.deleteFromPolicyMappingRecords(callCtx, attached);

    assertThat(
            impl.lookupPolicyMappingRecord(
                callCtx,
                POLICY_TARGET_CATALOG_ID,
                POLICY_TARGET_ID,
                policyTypeCode,
                POLICY_CATALOG_ID,
                POLICY_ID))
        .isNull();
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
}
