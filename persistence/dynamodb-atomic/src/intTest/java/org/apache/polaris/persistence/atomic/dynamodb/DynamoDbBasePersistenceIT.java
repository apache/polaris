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
package org.apache.polaris.persistence.atomic.dynamodb;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Map;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisChangeTrackingVersions;
import org.apache.polaris.core.entity.PolarisEntityId;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.persistence.EntityAlreadyExistsException;
import org.apache.polaris.core.persistence.RetryOnConcurrencyException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DynamoDbBasePersistenceIT extends AbstractDynamoDbTest {

  private static final long CATALOG = 7L;
  private static final long PARENT = 3L;
  private static final int TYPE = 2;

  private String table;
  private DynamoDbBasePersistence persistence;
  private PolarisCallContext callCtx;

  @BeforeEach
  void setUp() {
    table = uniqueTableName();
    DynamoDbSchema.createIfAbsent(client, table);
    persistence = new DynamoDbBasePersistence(client, table, "realm1");
    RealmContext realmContext = () -> "realm1";
    callCtx = new PolarisCallContext(realmContext, persistence);
  }

  @AfterEach
  void tearDown() {
    DynamoDbSchema.deleteIfExists(client, table);
  }

  @Test
  void createdEntityIsReadableByIdNameAndVersions() {
    PolarisBaseEntity sales = entity(persistence.generateNewId(callCtx), PARENT, "sales", 1);
    persistence.writeEntity(callCtx, sales, false, null);

    assertThat(persistence.lookupEntity(callCtx, CATALOG, sales.getId(), TYPE)).isEqualTo(sales);
    assertThat(persistence.lookupEntityByName(callCtx, CATALOG, PARENT, TYPE, "sales"))
        .isEqualTo(sales);
    assertThat(
            persistence.lookupEntities(
                callCtx,
                List.of(
                    new PolarisEntityId(CATALOG, sales.getId()),
                    new PolarisEntityId(CATALOG, 424242L))))
        .containsExactly(sales, null);
    assertThat(
            persistence.lookupEntityVersions(
                callCtx, List.of(new PolarisEntityId(CATALOG, sales.getId()))))
        .containsExactly(new PolarisChangeTrackingVersions(1, 1));
  }

  @Test
  void lookupsRejectMismatchedCoordinates() {
    PolarisBaseEntity sales = entity(10L, PARENT, "sales", 1);
    persistence.writeEntity(callCtx, sales, false, null);

    assertThat(persistence.lookupEntity(callCtx, CATALOG + 1, 10L, TYPE)).isNull();
    assertThat(persistence.lookupEntity(callCtx, CATALOG, 10L, TYPE + 1)).isNull();
    assertThat(persistence.lookupEntityByName(callCtx, CATALOG, PARENT, TYPE, "orders")).isNull();
    assertThat(persistence.lookupEntities(callCtx, List.of(new PolarisEntityId(CATALOG + 1, 10L))))
        .containsExactly((PolarisBaseEntity) null);
  }

  @Test
  void updateRequiresMatchingVersions() {
    PolarisBaseEntity v1 = entity(10L, PARENT, "sales", 1);
    persistence.writeEntity(callCtx, v1, false, null);
    PolarisBaseEntity v2 = withVersion(v1, 2, 1, Map.of("k", "v"));
    persistence.writeEntity(callCtx, v2, false, v1);

    // v1 is now stale: both a stale entityVersion and a stale grantRecordsVersion are rejected.
    assertThatThrownBy(
            () -> persistence.writeEntity(callCtx, withVersion(v1, 3, 1, Map.of()), false, v1))
        .isInstanceOf(RetryOnConcurrencyException.class);
    PolarisBaseEntity staleGrants = withVersion(v2, 2, 0, Map.of());
    assertThatThrownBy(
            () ->
                persistence.writeEntity(
                    callCtx, withVersion(v2, 3, 1, Map.of()), false, staleGrants))
        .isInstanceOf(RetryOnConcurrencyException.class);

    assertThat(persistence.lookupEntity(callCtx, CATALOG, 10L, TYPE)).isEqualTo(v2);
  }

  @Test
  void duplicateNameIsRejectedWithoutPartialWrites() {
    PolarisBaseEntity first = entity(10L, PARENT, "sales", 1);
    persistence.writeEntity(callCtx, first, false, null);

    assertThatThrownBy(
            () -> persistence.writeEntity(callCtx, entity(11L, PARENT, "sales", 1), false, null))
        .isInstanceOfSatisfying(
            EntityAlreadyExistsException.class,
            e -> assertThat(e.getExistingEntity().getId()).isEqualTo(10L));
    assertThat(persistence.lookupEntity(callCtx, CATALOG, 11L, TYPE)).isNull();
  }

  @Test
  void repeatedCreateReportsTheSameId() {
    PolarisBaseEntity sales = entity(10L, PARENT, "sales", 1);
    persistence.writeEntity(callCtx, sales, false, null);

    // The manager treats an existing entity with the same id as an idempotent retry.
    assertThatThrownBy(() -> persistence.writeEntity(callCtx, sales, false, null))
        .isInstanceOfSatisfying(
            EntityAlreadyExistsException.class,
            e -> assertThat(e.getExistingEntity().getId()).isEqualTo(10L));
  }

  @Test
  void renameMovesTheName() {
    PolarisBaseEntity sales = entity(10L, PARENT, "sales", 1);
    persistence.writeEntity(callCtx, sales, false, null);
    PolarisBaseEntity revenue = renamed(sales, PARENT, "revenue");
    persistence.writeEntity(callCtx, revenue, true, sales);

    assertThat(persistence.lookupEntityByName(callCtx, CATALOG, PARENT, TYPE, "sales")).isNull();
    assertThat(persistence.lookupEntityByName(callCtx, CATALOG, PARENT, TYPE, "revenue"))
        .isEqualTo(revenue);

    // The old name is free again.
    assertThatCode(
            () -> persistence.writeEntity(callCtx, entity(11L, PARENT, "sales", 1), false, null))
        .doesNotThrowAnyException();
  }

  @Test
  void reParentMovesTheName() {
    PolarisBaseEntity sales = entity(10L, PARENT, "sales", 1);
    persistence.writeEntity(callCtx, sales, false, null);
    PolarisBaseEntity moved = renamed(sales, PARENT + 1, "sales");
    persistence.writeEntity(callCtx, moved, true, sales);

    assertThat(persistence.lookupEntityByName(callCtx, CATALOG, PARENT, TYPE, "sales")).isNull();
    assertThat(persistence.lookupEntityByName(callCtx, CATALOG, PARENT + 1, TYPE, "sales"))
        .isEqualTo(moved);
  }

  @Test
  void renameOntoATakenNameIsRejected() {
    PolarisBaseEntity sales = entity(10L, PARENT, "sales", 1);
    PolarisBaseEntity orders = entity(11L, PARENT, "orders", 1);
    persistence.writeEntity(callCtx, sales, false, null);
    persistence.writeEntity(callCtx, orders, false, null);

    assertThatThrownBy(
            () -> persistence.writeEntity(callCtx, renamed(sales, PARENT, "orders"), true, sales))
        .isInstanceOfSatisfying(
            EntityAlreadyExistsException.class,
            e -> assertThat(e.getExistingEntity().getId()).isEqualTo(11L));
    assertThat(persistence.lookupEntityByName(callCtx, CATALOG, PARENT, TYPE, "sales"))
        .isEqualTo(sales);
  }

  @Test
  void nameChangedFlagWithUnchangedNameIsAPlainUpdate() {
    PolarisBaseEntity sales = entity(10L, PARENT, "sales", 1);
    persistence.writeEntity(callCtx, sales, false, null);
    PolarisBaseEntity updated = withVersion(sales, 2, 1, Map.of("k", "v"));

    persistence.writeEntity(callCtx, updated, true, sales);

    assertThat(persistence.lookupEntityByName(callCtx, CATALOG, PARENT, TYPE, "sales"))
        .isEqualTo(updated);
  }

  @Test
  void deleteFreesTheName() {
    PolarisBaseEntity sales = entity(10L, PARENT, "sales", 1);
    persistence.writeEntity(callCtx, sales, false, null);

    persistence.deleteEntity(callCtx, sales);

    assertThat(persistence.lookupEntity(callCtx, CATALOG, 10L, TYPE)).isNull();
    assertThat(persistence.lookupEntityByName(callCtx, CATALOG, PARENT, TYPE, "sales")).isNull();
    assertThatCode(() -> persistence.deleteEntity(callCtx, sales)).doesNotThrowAnyException();
  }

  @Test
  void deleteWithAStaleNameKeepsTheNewOwnersSentinel() {
    PolarisBaseEntity sales = entity(10L, PARENT, "sales", 1);
    persistence.writeEntity(callCtx, sales, false, null);
    persistence.writeEntity(callCtx, renamed(sales, PARENT, "revenue"), true, sales);
    PolarisBaseEntity newSales = entity(11L, PARENT, "sales", 1);
    persistence.writeEntity(callCtx, newSales, false, null);

    // The caller still holds the pre-rename copy, named "sales".
    persistence.deleteEntity(callCtx, sales);

    assertThat(persistence.lookupEntityByName(callCtx, CATALOG, PARENT, TYPE, "revenue")).isNull();
    assertThat(persistence.lookupEntityByName(callCtx, CATALOG, PARENT, TYPE, "sales"))
        .isEqualTo(newSales);
  }

  private static PolarisBaseEntity entity(long id, long parentId, String name, int version) {
    return new PolarisBaseEntity.Builder()
        .id(id)
        .catalogId(CATALOG)
        .parentId(parentId)
        .typeCode(TYPE)
        .subTypeCode(PolarisEntitySubType.NULL_SUBTYPE.getCode())
        .name(name)
        .entityVersion(version)
        .grantRecordsVersion(1)
        .propertiesAsMap(Map.of())
        .internalPropertiesAsMap(Map.of())
        .build();
  }

  private static PolarisBaseEntity withVersion(
      PolarisBaseEntity base,
      int entityVersion,
      int grantRecordsVersion,
      Map<String, String> props) {
    return new PolarisBaseEntity.Builder(base)
        .entityVersion(entityVersion)
        .grantRecordsVersion(grantRecordsVersion)
        .propertiesAsMap(props)
        .build();
  }

  private static PolarisBaseEntity renamed(PolarisBaseEntity base, long parentId, String name) {
    return new PolarisBaseEntity.Builder(base)
        .parentId(parentId)
        .name(name)
        .entityVersion(base.getEntityVersion() + 1)
        .build();
  }
}
