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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.entity.EntityNameLookupRecord;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.persistence.pagination.Page;
import org.apache.polaris.core.persistence.pagination.PageToken;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Listing reads the children index right after the writes. These tests rely on the Floci emulator
 * updating the index immediately; real DynamoDB updates it a moment later.
 */
class DynamoDbListingIT extends AbstractDynamoDbTest {

  private static final long CATALOG = 7L;
  private static final long NAMESPACE = 3L;
  private static final PolarisEntityType TABLE = PolarisEntityType.TABLE_LIKE;

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
  void listWithoutPaginationReturnsAllChildrenInIdOrder() {
    write(30L, NAMESPACE, "sales", PolarisEntitySubType.ICEBERG_TABLE);
    write(200L, NAMESPACE, "orders", PolarisEntitySubType.ICEBERG_TABLE);
    write(20L, NAMESPACE, "revenue", PolarisEntitySubType.ICEBERG_TABLE);
    write(40L, 99L, "elsewhere", PolarisEntitySubType.ICEBERG_TABLE);

    Page<EntityNameLookupRecord> page =
        list(PolarisEntitySubType.ANY_SUBTYPE, PageToken.readEverything());

    assertThat(page.items())
        .extracting(EntityNameLookupRecord::getId)
        .containsExactly(20L, 30L, 200L);
    assertThat(page.encodedResponseToken()).isNull();
  }

  @Test
  void pagesResumeAfterTheLastIdWithoutGapsOrRepeats() {
    List<Long> expected = new ArrayList<>();
    for (long id = 1; id <= 7; id++) {
      write(id * 10, NAMESPACE, "t" + id, PolarisEntitySubType.ICEBERG_TABLE);
      expected.add(id * 10);
    }

    List<Long> seen = new ArrayList<>();
    List<Integer> pageSizes = new ArrayList<>();
    PageToken token = PageToken.fromLimit(3);
    while (true) {
      Page<EntityNameLookupRecord> page = list(PolarisEntitySubType.ANY_SUBTYPE, token);
      page.items().forEach(r -> seen.add(r.getId()));
      pageSizes.add(page.items().size());
      String next = page.encodedResponseToken();
      if (next == null) {
        break;
      }
      token = PageToken.build(next, null, -1, () -> true);
    }

    assertThat(seen).containsExactlyElementsOf(expected);
    assertThat(pageSizes).containsExactly(3, 3, 1);
  }

  @Test
  void subtypeFilterStillFillsThePage() {
    // Views sit between the tables in id order, so the filter drops items mid-query.
    write(10L, NAMESPACE, "t1", PolarisEntitySubType.ICEBERG_TABLE);
    write(20L, NAMESPACE, "v1", PolarisEntitySubType.ICEBERG_VIEW);
    write(30L, NAMESPACE, "v2", PolarisEntitySubType.ICEBERG_VIEW);
    write(40L, NAMESPACE, "t2", PolarisEntitySubType.ICEBERG_TABLE);
    write(50L, NAMESPACE, "t3", PolarisEntitySubType.ICEBERG_TABLE);

    Page<EntityNameLookupRecord> first =
        list(PolarisEntitySubType.ICEBERG_TABLE, PageToken.fromLimit(2));
    assertThat(first.items()).extracting(EntityNameLookupRecord::getId).containsExactly(10L, 40L);

    Page<EntityNameLookupRecord> second =
        list(
            PolarisEntitySubType.ICEBERG_TABLE,
            PageToken.build(first.encodedResponseToken(), null, -1, () -> true));
    assertThat(second.items()).extracting(EntityNameLookupRecord::getId).containsExactly(50L);
    assertThat(second.encodedResponseToken()).isNull();
  }

  @Test
  void listFullEntitiesAppliesTheFilterAndTransformer() {
    write(10L, NAMESPACE, "keep_a", PolarisEntitySubType.ICEBERG_TABLE);
    write(20L, NAMESPACE, "drop_b", PolarisEntitySubType.ICEBERG_TABLE);
    write(30L, NAMESPACE, "keep_c", PolarisEntitySubType.ICEBERG_TABLE);
    write(40L, NAMESPACE, "keep_d", PolarisEntitySubType.ICEBERG_TABLE);

    Page<String> first =
        persistence.listFullEntities(
            callCtx,
            CATALOG,
            NAMESPACE,
            TABLE,
            PolarisEntitySubType.ANY_SUBTYPE,
            e -> e.getName().startsWith("keep"),
            PolarisBaseEntity::getName,
            PageToken.fromLimit(2));
    assertThat(first.items()).containsExactly("keep_a", "keep_c");

    Page<String> second =
        persistence.listFullEntities(
            callCtx,
            CATALOG,
            NAMESPACE,
            TABLE,
            PolarisEntitySubType.ANY_SUBTYPE,
            e -> e.getName().startsWith("keep"),
            PolarisBaseEntity::getName,
            PageToken.build(first.encodedResponseToken(), null, -1, () -> true));
    assertThat(second.items()).containsExactly("keep_d");
  }

  @Test
  void emptyParentListsNothing() {
    assertThat(list(PolarisEntitySubType.ANY_SUBTYPE, PageToken.readEverything()).items())
        .isEmpty();
    assertThat(list(PolarisEntitySubType.ANY_SUBTYPE, PageToken.fromLimit(5)).items()).isEmpty();
  }

  @Test
  void hasChildrenChecksTheGivenTypeOrAnyType() {
    assertThat(persistence.hasChildren(callCtx, null, CATALOG, NAMESPACE)).isFalse();

    write(10L, NAMESPACE, "sales", PolarisEntitySubType.ICEBERG_TABLE);

    assertThat(persistence.hasChildren(callCtx, TABLE, CATALOG, NAMESPACE)).isTrue();
    assertThat(persistence.hasChildren(callCtx, PolarisEntityType.NAMESPACE, CATALOG, NAMESPACE))
        .isFalse();
    assertThat(persistence.hasChildren(callCtx, null, CATALOG, NAMESPACE)).isTrue();
    assertThat(persistence.hasChildren(callCtx, null, CATALOG, 99L)).isFalse();
  }

  @Test
  void listAndHasChildrenIgnoreOtherRealms() {
    write(10L, NAMESPACE, "sales", PolarisEntitySubType.ICEBERG_TABLE);
    // Another realm in the same table, under the same catalog and parent ids.
    DynamoDbBasePersistence otherRealm = new DynamoDbBasePersistence(client, table, "realm2");
    PolarisCallContext otherCtx = new PolarisCallContext(() -> "realm2", otherRealm);
    otherRealm.writeEntity(
        otherCtx, entity(20L, NAMESPACE, "other", PolarisEntitySubType.ICEBERG_TABLE), false, null);
    otherRealm.writeEntity(
        otherCtx, entity(30L, 99L, "lonely", PolarisEntitySubType.ICEBERG_TABLE), false, null);

    assertThat(list(PolarisEntitySubType.ANY_SUBTYPE, PageToken.readEverything()).items())
        .extracting(EntityNameLookupRecord::getName)
        .containsExactly("sales");
    assertThat(persistence.hasChildren(callCtx, null, CATALOG, 99L)).isFalse();
    assertThat(otherRealm.hasChildren(otherCtx, null, CATALOG, 99L)).isTrue();
  }

  @Test
  void deletedChildIsNoLongerListed() {
    PolarisBaseEntity sales = write(10L, NAMESPACE, "sales", PolarisEntitySubType.ICEBERG_TABLE);
    persistence.deleteEntity(callCtx, sales);

    assertThat(list(PolarisEntitySubType.ANY_SUBTYPE, PageToken.readEverything()).items())
        .isEmpty();
    assertThat(persistence.hasChildren(callCtx, null, CATALOG, NAMESPACE)).isFalse();
  }

  private Page<EntityNameLookupRecord> list(PolarisEntitySubType subType, PageToken token) {
    return persistence.listEntities(callCtx, CATALOG, NAMESPACE, TABLE, subType, token);
  }

  private PolarisBaseEntity write(
      long id, long parentId, String name, PolarisEntitySubType subType) {
    PolarisBaseEntity entity = entity(id, parentId, name, subType);
    persistence.writeEntity(callCtx, entity, false, null);
    return entity;
  }

  private static PolarisBaseEntity entity(
      long id, long parentId, String name, PolarisEntitySubType subType) {
    return new PolarisBaseEntity.Builder()
        .id(id)
        .catalogId(CATALOG)
        .parentId(parentId)
        .typeCode(TABLE.getCode())
        .subTypeCode(subType.getCode())
        .name(name)
        .entityVersion(1)
        .grantRecordsVersion(1)
        .propertiesAsMap(Map.of())
        .internalPropertiesAsMap(Map.of())
        .build();
  }
}
