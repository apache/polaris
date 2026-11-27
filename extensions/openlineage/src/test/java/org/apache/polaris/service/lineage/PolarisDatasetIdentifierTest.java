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
package org.apache.polaris.service.lineage;

import static org.apache.polaris.service.lineage.LineageTestEvents.catalogFacet;
import static org.apache.polaris.service.lineage.LineageTestEvents.facets;
import static org.apache.polaris.service.lineage.LineageTestEvents.inputWithFacets;
import static org.apache.polaris.service.lineage.LineageTestEvents.sparkFacets;
import static org.apache.polaris.service.lineage.LineageTestEvents.symlink;
import static org.apache.polaris.service.lineage.LineageTestEvents.symlinksFacet;
import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.stream.Stream;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.polaris.service.lineage.LineageDatasetKey.CatalogFacet;
import org.apache.polaris.service.lineage.LineageDatasetKey.Role;
import org.apache.polaris.service.lineage.PolarisDatasetIdentifier.Identification;
import org.apache.polaris.service.lineage.PolarisDatasetIdentifier.Identification.Kind;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Tests for {@link PolarisDatasetIdentifier}.
 *
 * <p>The identity is read from the event's {@code catalog} and {@code symlinks} facets, shaped as
 * OpenLineage Spark 1.53.0 emits them for an Iceberg REST catalog. The dataset's own {@code
 * namespace} and {@code name} are a storage location in that case and are never consulted.
 */
class PolarisDatasetIdentifierTest {

  private static final String URI = "http://polaris:8181/api/catalog";
  private static final String CATALOG = "quickstart_catalog";

  private static Identification identify(ObjectNode facets) {
    return PolarisDatasetIdentifier.identify(
        LineageDatasetKey.of(Role.INPUT, inputWithFacets("s3://bucket", "wh/ns/tbl", facets)));
  }

  private static Identification identifySpark(String qualifiedTable) {
    return identify(sparkFacets(URI, CATALOG, qualifiedTable));
  }

  private static Identification candidate(String catalog, TableIdentifier table) {
    return new Identification(Kind.POLARIS_CANDIDATE, catalog, table);
  }

  private static Identification unaddressable(String catalog) {
    return new Identification(Kind.UNADDRESSABLE_POLARIS_CLAIM, catalog, null);
  }

  private static final Identification EXTERNAL = new Identification(Kind.EXTERNAL, null, null);

  @Test
  void aSparkTableInAnIcebergRestCatalogIsACandidate() {
    assertThat(identifySpark("ns.tbl"))
        .isEqualTo(candidate(CATALOG, TableIdentifier.of(Namespace.of("ns"), "tbl")));
  }

  @Test
  void aMultiLevelNamespaceIsKept() {
    assertThat(identifySpark("a.b.tbl"))
        .isEqualTo(candidate(CATALOG, TableIdentifier.of(Namespace.of("a", "b"), "tbl")));
  }

  static Stream<Arguments> quotedNames() {
    return Stream.of(
        // Spark quotes a part that is not a plain identifier.
        Arguments.of("`my-ns`.tbl", TableIdentifier.of(Namespace.of("my-ns"), "tbl")),
        Arguments.of("ns.`my-tbl`", TableIdentifier.of(Namespace.of("ns"), "my-tbl")),
        // A dot inside a quoted part belongs to the part.
        Arguments.of("`a.b`.tbl", TableIdentifier.of(Namespace.of("a.b"), "tbl")),
        // A doubled backtick inside a quoted part is one backtick.
        Arguments.of("`it``s`.tbl", TableIdentifier.of(Namespace.of("it`s"), "tbl")),
        Arguments.of("`a`.`b`.`c`", TableIdentifier.of(Namespace.of("a", "b"), "c")));
  }

  @ParameterizedTest
  @MethodSource("quotedNames")
  void backtickQuotedPartsAreUnquoted(String qualifiedTable, TableIdentifier expected) {
    assertThat(identifySpark(qualifiedTable)).isEqualTo(candidate(CATALOG, expected));
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        // Too short: a table path needs a namespace level and a table.
        "orders",
        "",
        // Blank parts.
        "a..b",
        ".tbl",
        "ns.",
        " .tbl",
        "ns. ",
        // Not in the form Spark emits.
        "`ns.tbl",
        "ns.`tbl`x",
        "`ns`x.tbl",
        "n`s.tbl",
        // The null byte cannot be in an Iceberg namespace.
        "a\0b.tbl"
      })
  void aCatalogWithoutAUsableTablePathIsAnUnaddressableClaim(String qualifiedTable) {
    assertThat(identifySpark(qualifiedTable)).isEqualTo(unaddressable(CATALOG));
  }

  @Test
  void aNullTableNameIsAnUnaddressableClaim() {
    assertThat(
            identify(
                facets(
                    catalogFacet("iceberg", "rest", URI, CATALOG),
                    symlinksFacet(symlink(URI, null, "TABLE")))))
        .isEqualTo(unaddressable(CATALOG));
  }

  @Test
  void aDatasetWithoutACatalogFacetIsExternal() {
    // The symlink alone is not enough: nothing says which catalog it is a name in.
    assertThat(identify(facets(null, symlinksFacet(symlink(URI, "ns.tbl", "TABLE")))))
        .isEqualTo(EXTERNAL);
    assertThat(identify(facets(null, null))).isEqualTo(EXTERNAL);
  }

  static Stream<Arguments> notAnIcebergRestCatalog() {
    return Stream.of(
        Arguments.of("iceberg", "hive"),
        Arguments.of("iceberg", "glue"),
        Arguments.of("delta", "rest"),
        Arguments.of(null, "rest"),
        Arguments.of("iceberg", null));
  }

  @ParameterizedTest
  @MethodSource("notAnIcebergRestCatalog")
  void aCatalogThatIsNotAnIcebergRestCatalogIsExternal(String framework, String type) {
    // For Hive or Glue the facet's warehouseUri is a storage path, not a catalog name.
    assertThat(
            identify(
                facets(
                    catalogFacet(framework, type, URI, CATALOG),
                    symlinksFacet(symlink(URI, "ns.tbl", "TABLE")))))
        .isEqualTo(EXTERNAL);
  }

  @Test
  void theCatalogTypeAndFrameworkAreMatchedIgnoringCase() {
    assertThat(
            identify(
                facets(
                    catalogFacet("Iceberg", "REST", URI, CATALOG),
                    symlinksFacet(symlink(URI, "ns.tbl", "table")))))
        .isEqualTo(candidate(CATALOG, TableIdentifier.of(Namespace.of("ns"), "tbl")));
  }

  @ParameterizedTest
  @NullAndEmptySource
  @ValueSource(strings = {" ", "\t", "a\0b"})
  void aCatalogFacetWithoutAUsableCatalogNameIsExternal(String warehouse) {
    assertThat(
            identify(
                facets(
                    catalogFacet("iceberg", "rest", URI, warehouse),
                    symlinksFacet(symlink(URI, "ns.tbl", "TABLE")))))
        .isEqualTo(EXTERNAL);
  }

  @Test
  void aCatalogWithoutATableSymlinkIsAnUnaddressableClaim() {
    assertThat(identify(facets(catalogFacet("iceberg", "rest", URI, CATALOG), null)))
        .isEqualTo(unaddressable(CATALOG));
    assertThat(
            identify(
                facets(
                    catalogFacet("iceberg", "rest", URI, CATALOG),
                    symlinksFacet(symlink("hive://metastore:9083", "db.tbl", "TABLE")))))
        .isEqualTo(unaddressable(CATALOG));
  }

  @Test
  void onlyATableSymlinkNamesTheTable() {
    assertThat(
            identify(
                facets(
                    catalogFacet("iceberg", "rest", URI, CATALOG),
                    symlinksFacet(symlink(URI, "ns.tbl", "LOCATION")))))
        .isEqualTo(unaddressable(CATALOG));
  }

  @Test
  void aSymlinkOnAnotherEndpointDoesNotNameTheTable() {
    // Its name is a name in some other catalog, so it says nothing about this one.
    assertThat(
            identify(
                facets(
                    catalogFacet("iceberg", "rest", URI, CATALOG),
                    symlinksFacet(symlink("http://other:8181/api/catalog", "ns.tbl", "TABLE")))))
        .isEqualTo(unaddressable(CATALOG));
  }

  @Test
  void aCatalogFacetWithoutAnEndpointIsAnUnaddressableClaim() {
    assertThat(
            identify(
                facets(
                    catalogFacet("iceberg", "rest", null, CATALOG),
                    symlinksFacet(symlink(URI, "ns.tbl", "TABLE")))))
        .isEqualTo(unaddressable(CATALOG));
  }

  @Test
  void conflictingTableSymlinksAreAnUnaddressableClaim() {
    // Two different tables on one dataset: neither can be trusted over the other.
    assertThat(
            identify(
                facets(
                    catalogFacet("iceberg", "rest", URI, CATALOG),
                    symlinksFacet(
                        symlink(URI, "ns.tbl", "TABLE"), symlink(URI, "ns.other", "TABLE")))))
        .isEqualTo(unaddressable(CATALOG));
  }

  @Test
  void aUsableAndAnUnusableTableSymlinkTogetherAreAnUnaddressableClaim() {
    // An extra symlink can add a drop but never remove one.
    assertThat(
            identify(
                facets(
                    catalogFacet("iceberg", "rest", URI, CATALOG),
                    symlinksFacet(symlink(URI, "ns.tbl", "TABLE"), symlink(URI, "tbl", "TABLE")))))
        .isEqualTo(unaddressable(CATALOG));
  }

  @Test
  void theSameTableSymlinkRepeatedIsStillACandidate() {
    assertThat(
            identify(
                facets(
                    catalogFacet("iceberg", "rest", URI, CATALOG),
                    symlinksFacet(
                        symlink(URI, "ns.tbl", "TABLE"), symlink(URI, "ns.tbl", "TABLE")))))
        .isEqualTo(candidate(CATALOG, TableIdentifier.of(Namespace.of("ns"), "tbl")));
  }

  @Test
  void theHostIsNotPartOfTheIdentity() {
    // Polaris cannot tell from an event which server a URI belongs to, so the same catalog,
    // namespace and table on another host is the same candidate. The authorization pass decides
    // whether the catalog is this server's by resolving it.
    Identification local = identify(sparkFacets("https://local/api/catalog", CATALOG, "ns.tbl"));
    Identification remote = identify(sparkFacets("https://remote/api/catalog", CATALOG, "ns.tbl"));

    assertThat(remote)
        .isEqualTo(local)
        .isEqualTo(candidate(CATALOG, TableIdentifier.of(Namespace.of("ns"), "tbl")));
  }

  @Test
  void theDatasetsOwnNamespaceAndNameAreNeverRead() {
    // A name that looks like catalog.namespace.table under an unrelated namespace stays external:
    // Kafka topics and BigQuery tables are routinely dotted and are not Polaris entities.
    assertThat(
            PolarisDatasetIdentifier.identify(
                LineageDatasetKey.of(
                    Role.INPUT,
                    inputWithFacets("kafka://broker", "cat.ns.tbl", facets(null, null)))))
        .isEqualTo(EXTERNAL);

    // And a name that points somewhere else does not override what the facets say.
    assertThat(
            PolarisDatasetIdentifier.identify(
                LineageDatasetKey.of(
                    Role.INPUT,
                    inputWithFacets(
                        "kafka://broker", "other.a.b", sparkFacets(URI, CATALOG, "x.y")))))
        .isEqualTo(candidate(CATALOG, TableIdentifier.of(Namespace.of("x"), "y")));
  }

  @Test
  void aKeyWithNothingInItIsExternalAndNeverThrows() {
    assertThat(PolarisDatasetIdentifier.identify(new LineageDatasetKey(Role.INPUT, null, null)))
        .isEqualTo(EXTERNAL);
    assertThat(
            PolarisDatasetIdentifier.identify(
                new LineageDatasetKey(
                    Role.OUTPUT, null, null, new CatalogFacet(null, null, null, null), null)))
        .isEqualTo(EXTERNAL);
  }
}
