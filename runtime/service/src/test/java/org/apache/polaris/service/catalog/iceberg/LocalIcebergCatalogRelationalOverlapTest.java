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
package org.apache.polaris.service.catalog.iceberg;

import static org.apache.polaris.service.admin.PolarisAuthzTestBase.SCHEMA;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.polaris.core.entity.NamespaceEntity;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PolarisEntityConstants;
import org.apache.polaris.core.entity.PolarisEntityCore;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.table.IcebergTableLikeEntity;
import org.apache.polaris.core.persistence.dao.entity.EntityResult;
import org.apache.polaris.service.Profiles;
import org.junit.jupiter.api.Test;

/**
 * JDBC/relational-backed overlap test. Inherits the assertions from {@link
 * AbstractLocalIcebergCatalogOverlapTest} and runs them against the relational-jdbc metastore (H2)
 * with overlap enforcement enabled.
 */
@QuarkusTest
@TestProfile(LocalIcebergCatalogRelationalOverlapTest.RelationalOverlapProfile.class)
public class LocalIcebergCatalogRelationalOverlapTest
    extends AbstractLocalIcebergCatalogOverlapTest {

  /**
   * Profile that runs the relational-jdbc backend (H2 in-memory) with location-overlap enforcement
   * and the optimized sibling check enabled.
   */
  public static class RelationalOverlapProfile extends Profiles.DefaultIcebergCatalogProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      Map<String, String> overrides = new HashMap<>(super.getConfigOverrides());
      overrides.put("polaris.features.\"ALLOW_TABLE_LOCATION_OVERLAP\"", "false");
      overrides.put("polaris.features.\"OPTIMIZED_SIBLING_CHECK\"", "true");
      overrides.put("polaris.features.\"ALLOW_OPTIMIZED_SIBLING_CHECK\"", "true");
      overrides.put("polaris.persistence.type", "relational-jdbc");
      overrides.put("polaris.persistence.auto-bootstrap-types", "relational-jdbc");
      overrides.put("quarkus.datasource.db-kind", "h2");
      overrides.put(
          "quarkus.datasource.jdbc.url",
          "jdbc:h2:mem:test;DB_CLOSE_DELAY=-1;MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE");
      return overrides;
    }
  }

  /**
   * Regression guard for the QueryGenerator trailing-slash false-negative. It is kept JDBC-specific
   * on purpose: the failure mode lives in {@code QueryGenerator}'s ancestor-equality terms, which
   * only the relational-jdbc optimized sibling check exercises. The in-memory and NoSQL profiles
   * use different overlap paths, so running this there would pass regardless and give false
   * confidence.
   */
  @Test
  public void testParentPrefixOverlapWithTrailingSlashMismatch() {
    // The catalog now always stores locations with a trailing slash
    // (ADD_TRAILING_SLASH_TO_LOCATION was removed). Simulate legacy slash-less data
    // by rewriting the stored parent location directly, then verify the optimized sibling check
    // still detects overlaps against it: ancestor equality terms used to be slash-terminated only,
    // so a slash-less location_without_scheme was missed.
    Namespace ns = Namespace.of("ns-for-trailing-slash-overlap");
    catalog().createNamespace(ns);

    TableIdentifier parentTable = TableIdentifier.of(ns, "trailing-parent");
    String parentLoc = STORAGE_LOCATION + "/trailing-overlap/parent";
    assertThat(parentLoc).doesNotEndWith("/");
    catalog().buildTable(parentTable, SCHEMA).withLocation(parentLoc).create();

    stripStoredBaseLocationTrailingSlash(ns, parentTable);

    // Guardrail: the stored location must be slash-less so overlap checks exercise the
    // non-slash-terminated location_without_scheme path that QueryGenerator must handle.
    assertThat(storedTableBaseLocation(ns, parentTable))
        .as("parent must remain slash-less so overlap uses non-slash-terminated stored location")
        .isEqualTo(parentLoc)
        .doesNotEndWith("/");

    // Creating a child table under the slash-less parent location must be rejected.
    TableIdentifier childTable = TableIdentifier.of(ns, "trailing-child");
    String childLoc = STORAGE_LOCATION + "/trailing-overlap/parent/child";
    assertThatThrownBy(
            () -> catalog().buildTable(childTable, SCHEMA).withLocation(childLoc).create())
        .isInstanceOf(ForbiddenException.class)
        .hasMessageContaining("Unable to create entity at location")
        .hasMessageContaining("conflicts with existing table or namespace");

    // The same must hold when the slash-terminated form of the same prefix is used.
    TableIdentifier siblingTable = TableIdentifier.of(ns, "trailing-sibling");
    String siblingLoc = STORAGE_LOCATION + "/trailing-overlap/parent/";
    assertThatThrownBy(
            () -> catalog().buildTable(siblingTable, SCHEMA).withLocation(siblingLoc).create())
        .isInstanceOf(ForbiddenException.class)
        .hasMessageContaining("Unable to create entity at location")
        .hasMessageContaining("conflicts with existing table or namespace");
  }

  /**
   * Regression for the {@code setProperties} normalization path. A namespace stored before Polaris
   * always appended a trailing slash keeps a slash-less location. Updating an unrelated property
   * must not be rejected: previously the slash-only normalization flipped {@code locationChanged}
   * to true and ran the overlap check, which under the optimized sibling check matched the
   * namespace's own still-persisted slash-less row (it does not exclude the current entity). The
   * update must succeed and the stored location must be re-normalized to end with a slash. Kept
   * JDBC-specific because it depends on the optimized sibling check backed by {@code
   * QueryGenerator}.
   */
  @Test
  public void testNamespacePropertyUpdateNormalizesLegacySlashlessLocation() {
    Namespace ns = Namespace.of("ns-legacy-slashless");
    // Let Polaris derive the location (custom namespace locations are disabled in this profile).
    catalog().createNamespace(ns);

    // On create Polaris stores the location slash-terminated.
    String created = storedNamespaceBaseLocation(ns);
    assertThat(created).endsWith("/");

    // Simulate a row written before that normalization existed.
    stripStoredNamespaceBaseLocationTrailingSlash(ns);
    String legacy = storedNamespaceBaseLocation(ns);
    assertThat(legacy).doesNotEndWith("/");
    assertThat(created).isEqualTo(legacy + "/");

    // Updating a non-location property must succeed (not be flagged as a self-overlap) and must
    // re-normalize the stored location.
    assertThatCode(() -> catalog().setProperties(ns, Map.of("some-key", "some-value")))
        .doesNotThrowAnyException();
    assertThat(storedNamespaceBaseLocation(ns)).endsWith("/");
  }

  private String storedTableBaseLocation(Namespace ns, TableIdentifier table) {
    return IcebergTableLikeEntity.of(readStoredTableLike(ns, table)).getBaseLocation();
  }

  private String storedNamespaceBaseLocation(Namespace ns) {
    return NamespaceEntity.of(readStoredNamespace(ns)).getBaseLocation();
  }

  private PolarisEntity readStoredNamespace(Namespace ns) {
    EntityResult result =
        metaStoreManager.readEntityByName(
            polarisContext,
            List.of(PolarisEntity.toCore(catalogEntity)),
            PolarisEntityType.NAMESPACE,
            PolarisEntitySubType.NULL_SUBTYPE,
            ns.level(0));
    assertThat(result.isSuccess()).isTrue();
    return PolarisEntity.of(result.getEntity());
  }

  /**
   * Rewrites the stored base location of {@code ns} without its trailing slash, simulating a
   * namespace written before Polaris always appended one.
   */
  private void stripStoredNamespaceBaseLocationTrailingSlash(Namespace ns) {
    PolarisEntity nsEntity = readStoredNamespace(ns);
    Map<String, String> properties = new HashMap<>(nsEntity.getPropertiesAsMap());
    String baseLocation = properties.get(PolarisEntityConstants.ENTITY_BASE_LOCATION);
    assertThat(baseLocation).endsWith("/");
    properties.put(
        PolarisEntityConstants.ENTITY_BASE_LOCATION,
        baseLocation.substring(0, baseLocation.length() - 1));
    PolarisEntity stripped = new PolarisEntity.Builder(nsEntity).setProperties(properties).build();
    EntityResult result =
        metaStoreManager.updateEntityPropertiesIfNotChanged(
            polarisContext, List.of(PolarisEntity.toCore(catalogEntity)), stripped);
    assertThat(result.isSuccess()).isTrue();
  }

  /**
   * Rewrites the stored base location of {@code table} without its trailing slash, simulating data
   * written before Polaris always appended one.
   */
  private void stripStoredBaseLocationTrailingSlash(Namespace ns, TableIdentifier table) {
    PolarisEntity tableEntity = readStoredTableLike(ns, table);
    IcebergTableLikeEntity tableLike = IcebergTableLikeEntity.of(tableEntity);
    String baseLocation = tableLike.getBaseLocation();
    assertThat(baseLocation).endsWith("/");
    IcebergTableLikeEntity stripped =
        new IcebergTableLikeEntity.Builder(tableLike)
            .setBaseLocation(baseLocation.substring(0, baseLocation.length() - 1))
            .build();
    EntityResult result =
        metaStoreManager.updateEntityPropertiesIfNotChanged(
            polarisContext, tableLikeCatalogPath(ns), stripped);
    assertThat(result.isSuccess()).isTrue();
  }

  private PolarisEntity readStoredTableLike(Namespace ns, TableIdentifier table) {
    EntityResult result =
        metaStoreManager.readEntityByName(
            polarisContext,
            tableLikeCatalogPath(ns),
            PolarisEntityType.TABLE_LIKE,
            PolarisEntitySubType.ICEBERG_TABLE,
            table.name());
    assertThat(result.isSuccess()).isTrue();
    return PolarisEntity.of(result.getEntity());
  }

  private List<PolarisEntityCore> tableLikeCatalogPath(Namespace ns) {
    assertThat(ns.length()).as("test namespaces must be single-level").isEqualTo(1);
    EntityResult result =
        metaStoreManager.readEntityByName(
            polarisContext,
            List.of(PolarisEntity.toCore(catalogEntity)),
            PolarisEntityType.NAMESPACE,
            PolarisEntitySubType.NULL_SUBTYPE,
            ns.level(0));
    assertThat(result.isSuccess()).isTrue();
    return List.of(
        PolarisEntity.toCore(catalogEntity),
        PolarisEntity.toCore(PolarisEntity.of(result.getEntity())));
  }
}
