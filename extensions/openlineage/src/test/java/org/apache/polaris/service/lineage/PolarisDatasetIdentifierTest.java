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

import static org.apache.polaris.service.lineage.PolarisDatasetIdentifier.Identification.Kind.EXTERNAL;
import static org.apache.polaris.service.lineage.PolarisDatasetIdentifier.Identification.Kind.POLARIS;
import static org.apache.polaris.service.lineage.PolarisDatasetIdentifier.Identification.Kind.UNADDRESSABLE_POLARIS_CLAIM;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.service.lineage.PolarisDatasetIdentifier.Identification;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Tests for {@link PolarisDatasetIdentifier}, the security boundary of lineage ingest.
 *
 * <p>The distinction these tests defend is that {@code EXTERNAL} means "no Polaris identity is
 * derivable", so an external dataset can never be read back as naming a Polaris entity, while
 * anything that looks Polaris-addressed but is malformed must be {@code
 * UNADDRESSABLE_POLARIS_CLAIM} — dropped, not recorded unauthenticated beside the real entity.
 */
class PolarisDatasetIdentifierTest {

  private static final String CONFIGURED_NAMESPACE = "s3://warehouse";

  /** A default Polaris deployment's Iceberg REST catalog endpoint. */
  private static final String POLARIS_CATALOG_URI = "http://polaris:8181/api/catalog";

  private static final String MAPPED_CATALOG = "prod_catalog";

  private final PolarisDatasetIdentifier identifier =
      new PolarisDatasetIdentifier(Map.of(CONFIGURED_NAMESPACE, MAPPED_CATALOG));

  private static final char NUL = '\0';

  @Nested
  class ConfiguredNamespace {

    @Test
    void mapsToTheConfiguredCatalogAndReadsTheWholeNameAsAPathWithinIt() {
      Identification identification = identifier.identify(CONFIGURED_NAMESPACE, "sales.orders");

      assertThat(identification.kind()).isEqualTo(POLARIS);
      assertThat(identification.identity())
          .isEqualTo(
              new PolarisDatasetIdentity(
                  MAPPED_CATALOG, TableIdentifier.of(Namespace.of("sales"), "orders")));
    }

    @Test
    void supportsNestedNamespaces() {
      Identification identification =
          identifier.identify(CONFIGURED_NAMESPACE, "sales.eu.daily.orders");

      assertThat(identification.kind()).isEqualTo(POLARIS);
      assertThat(identification.identity().catalog()).isEqualTo(MAPPED_CATALOG);
      assertThat(identification.identity().table())
          .isEqualTo(TableIdentifier.of(Namespace.of("sales", "eu", "daily"), "orders"));
    }

    @Test
    void doesNotApplyTheThreeSegmentFallbackInsideAConfiguredNamespace() {
      // Three segments here are namespace.namespace.table within the mapped catalog, NOT
      // catalog.namespace.table. Reading it the other way would authorize against the wrong
      // catalog.
      Identification identification = identifier.identify(CONFIGURED_NAMESPACE, "a.b.c");

      assertThat(identification.identity().catalog()).isEqualTo(MAPPED_CATALOG);
      assertThat(identification.identity().table())
          .isEqualTo(TableIdentifier.of(Namespace.of("a", "b"), "c"));
    }

    @Test
    void aNameWithNoNamespaceLevelIsAnUnaddressableClaimRatherThanExternal() {
      // Polaris has no tables directly under a catalog, so "orders" cannot be addressed. It still
      // claimed a mapped catalog, so recording it as an external node would put an unauthenticated
      // node bearing a Polaris catalog's namespace into the graph.
      assertThat(identifier.identify(CONFIGURED_NAMESPACE, "orders").kind())
          .isEqualTo(UNADDRESSABLE_POLARIS_CLAIM);
    }

    @ParameterizedTest
    @ValueSource(strings = {"", " ", ".", "..", "a.", ".a", "a..b", "a. .b", "a.b."})
    void aNameWithABlankSegmentIsAnUnaddressableClaim(String name) {
      assertThat(identifier.identify(CONFIGURED_NAMESPACE, name).kind())
          .isEqualTo(UNADDRESSABLE_POLARIS_CLAIM);
    }

    @Test
    void aNullNameIsAnUnaddressableClaim() {
      assertThat(identifier.identify(CONFIGURED_NAMESPACE, null).kind())
          .isEqualTo(UNADDRESSABLE_POLARIS_CLAIM);
    }

    @Test
    void namespaceMatchingIsExactAndCaseSensitive() {
      // Polaris entity names are case-sensitive. A case-skewed namespace must not be treated as the
      // configured one; it falls through to the name-shape rule instead.
      assertThat(identifier.identify("S3://WAREHOUSE", "sales.orders").kind()).isEqualTo(EXTERNAL);
      assertThat(identifier.identify(CONFIGURED_NAMESPACE + "/", "sales.orders").kind())
          .isEqualTo(EXTERNAL);
      assertThat(identifier.identify(" " + CONFIGURED_NAMESPACE, "sales.orders").kind())
          .isEqualTo(EXTERNAL);
    }
  }

  @Nested
  class NamespaceDecidesNotNameShape {

    /**
     * The bug this guards: a dotted name under an unrelated namespace used to be claimed as a
     * Polaris table on segment count alone, which sent genuinely external datasets down the
     * resolve-or-drop path and dropped them from the graph entirely.
     */
    @ParameterizedTest
    @ValueSource(
        strings = {
          "kafka://broker",
          "s3://some-bucket",
          "postgres://db:5432",
          "bigquery",
          "snowflake://acct.snowflakecomputing.com",
          "http://example.com/not/polaris",
          "https://polaris.example.com/api/other"
        })
    void aDottedNameUnderAnUnmappedNonPolarisNamespaceStaysExternal(String datasetNamespace) {
      assertThat(identifier.identify(datasetNamespace, "a.b.c").kind()).isEqualTo(EXTERNAL);
      assertThat(identifier.identify(datasetNamespace, "a.b.c.d.e").kind()).isEqualTo(EXTERNAL);
    }

    @ParameterizedTest
    @ValueSource(
        strings = {
          "http://polaris:8181/api/catalog",
          "https://polaris.example.com/api/catalog",
          "http://polaris:8181/api/catalog/",
          "HTTP://POLARIS:8181/api/catalog"
        })
    void aPolarisCatalogEndpointIsRecognizedWithoutConfiguration(String datasetNamespace) {
      assertThat(identifier.identify(datasetNamespace, "cat.ns.tbl").kind()).isEqualTo(POLARIS);
    }

    @Test
    void aMappedNamespaceWinsRegardlessOfItsShape() {
      // The config map is authoritative: a deployment may serve the catalog from any path, so URI
      // recognition is only a fallback for an unmapped namespace.
      PolarisDatasetIdentifier mapped =
          new PolarisDatasetIdentifier(Map.of("kafka://broker", "prod"));

      assertThat(mapped.identify("kafka://broker", "ns.tbl").kind()).isEqualTo(POLARIS);
      assertThat(mapped.identify("kafka://broker", "ns.tbl").identity().catalog())
          .isEqualTo("prod");
    }
  }

  @Nested
  class UnconfiguredNamespace {

    @Test
    void threeSegmentsAreReadAsCatalogNamespaceTable() {
      Identification identification = identifier.identify(POLARIS_CATALOG_URI, "cat.ns.tbl");

      assertThat(identification.kind()).isEqualTo(POLARIS);
      assertThat(identification.identity())
          .isEqualTo(
              new PolarisDatasetIdentity("cat", TableIdentifier.of(Namespace.of("ns"), "tbl")));
    }

    @Test
    void moreThanThreeSegmentsPutTheMiddleOnesInTheNamespace() {
      Identification identification = identifier.identify(POLARIS_CATALOG_URI, "cat.a.b.c.tbl");

      assertThat(identification.identity())
          .isEqualTo(
              new PolarisDatasetIdentity(
                  "cat", TableIdentifier.of(Namespace.of("a", "b", "c"), "tbl")));
    }

    @ParameterizedTest
    @ValueSource(strings = {"orders", "sales.orders", "", " "})
    void fewerThanThreeSegmentsIsExternal(String name) {
      assertThat(identifier.identify(POLARIS_CATALOG_URI, name).kind()).isEqualTo(EXTERNAL);
    }

    @Test
    void aNullNameIsExternal() {
      assertThat(identifier.identify(POLARIS_CATALOG_URI, null).kind()).isEqualTo(EXTERNAL);
    }

    @Test
    void aNullOrBlankNamespaceIsExternalWhateverTheNameLooksLike() {
      // With no namespace there is nothing to identify the dataset as Polaris, and the name's shape
      // alone is not evidence.
      assertThat(identifier.identify(null, "cat.ns.tbl").kind()).isEqualTo(EXTERNAL);
      assertThat(identifier.identify("", "cat.ns.tbl").kind()).isEqualTo(EXTERNAL);
      assertThat(identifier.identify("   ", "cat.ns.tbl").kind()).isEqualTo(EXTERNAL);
    }

    @ParameterizedTest
    @ValueSource(strings = {"...", "a..b", ".ns.tbl", "cat..tbl", "cat.ns.", "cat. .tbl", "a.b."})
    void aThreeSegmentNameWithABlankSegmentIsDroppedNotRecordedAsExternal(String name) {
      // The near-miss forgery vector: "cat..tbl" looks like it addresses Polaris. If it were called
      // external it would be persisted with no privilege check, sitting beside the real "cat.tbl".
      assertThat(identifier.identify(POLARIS_CATALOG_URI, name).kind())
          .isEqualTo(UNADDRESSABLE_POLARIS_CLAIM);
    }

    @Test
    void aNameThatLooksPolarisIshButHasTooFewSegmentsIsExternalNotDropped() {
      // Two segments is the schema.table convention of nearly every non-Polaris system, and Polaris
      // cannot address it, so it is genuinely external rather than a malformed Polaris claim.
      assertThat(identifier.identify("postgres://db:5432", "public.users").kind())
          .isEqualTo(EXTERNAL);
    }

    @Test
    void caseIsPreservedExactlyInTheDerivedIdentity() {
      Identification identification = identifier.identify(POLARIS_CATALOG_URI, "Cat.NS.Tbl");

      assertThat(identification.identity().catalog()).isEqualTo("Cat");
      assertThat(identification.identity().table())
          .isEqualTo(TableIdentifier.of(Namespace.of("NS"), "Tbl"));
    }
  }

  @Nested
  class AdversarialInput {

    @Test
    void aNullByteInAnySegmentIsAnUnaddressableClaim() {
      // Iceberg's Namespace.of throws on the null byte. The predicate must classify, not throw.
      assertThat(identifier.identify(POLARIS_CATALOG_URI, "cat.n" + NUL + "s.tbl").kind())
          .isEqualTo(UNADDRESSABLE_POLARIS_CLAIM);
      assertThat(identifier.identify(POLARIS_CATALOG_URI, "ca" + NUL + "t.ns.tbl").kind())
          .isEqualTo(UNADDRESSABLE_POLARIS_CLAIM);
      assertThat(identifier.identify(POLARIS_CATALOG_URI, "cat.ns.tb" + NUL + "l").kind())
          .isEqualTo(UNADDRESSABLE_POLARIS_CLAIM);
    }

    @ParameterizedTest
    @ValueSource(
        strings = {
          "cat.ns.tbl ",
          "cat.ns. tbl",
          "cat.ns.tbl\n",
          "cat.ns.tbl\t",
          "cat.ns.../../etc/passwd",
          "cat.ns.tbl;DROP TABLE",
          "cat.ns.tbl'--",
          "cat.ns.été",
          "cat.ns.🚀",
          "cat.\0\0.tbl"
        })
    void unusualCharactersAreClassifiedWithoutThrowing(String name) {
      assertThatCode(() -> identifier.identify(POLARIS_CATALOG_URI, name))
          .doesNotThrowAnyException();
    }

    @Test
    void unusualButNonBlankCharactersStayPolarisNativeAndAreNeverSilentlyNormalized() {
      // Whitespace and punctuation are legal in a Polaris entity name, so these remain Polaris
      // claims that must resolve. Trimming or rewriting them here would authorize one name while
      // addressing another.
      Identification identification = identifier.identify(POLARIS_CATALOG_URI, "cat.ns.tbl ");

      assertThat(identification.kind()).isEqualTo(POLARIS);
      assertThat(identification.identity().table().name()).isEqualTo("tbl ");
    }

    @Test
    void aVeryLongNameIsClassifiedWithoutThrowing() {
      String name = "cat.ns." + "t".repeat(100_000);
      assertThat(identifier.identify(POLARIS_CATALOG_URI, name).kind()).isEqualTo(POLARIS);
    }

    @Test
    void aNameOfOnlyDotsIsDropped() {
      assertThat(identifier.identify(POLARIS_CATALOG_URI, ".".repeat(50)).kind())
          .isEqualTo(UNADDRESSABLE_POLARIS_CLAIM);
    }
  }

  @Nested
  class ConfigParsing {

    @Test
    void parsesEntriesAndSplitsOnTheFirstEqualsOnly() {
      // A dataset namespace is a URI and may contain '=' in a query string.
      Map<String, String> parsed =
          PolarisDatasetIdentifier.parseNamespaceCatalogs(
              List.of("s3://bucket=cat_a", "http://h/p?x=1=cat_b"));

      assertThat(parsed)
          .containsExactlyInAnyOrderEntriesOf(
              Map.of("s3://bucket", "cat_a", "http://h/p?x", "1=cat_b"));
    }

    @Test
    void skipsMalformedBlankAndNullEntries() {
      Map<String, String> parsed =
          PolarisDatasetIdentifier.parseNamespaceCatalogs(
              Arrays.asList("no-separator", "=cat", "ns=", "  =cat", "ns=  ", null, "good=cat"));

      assertThat(parsed).containsExactly(Map.entry("good", "cat"));
    }

    @Test
    void firstEntryForANamespaceWins() {
      // A duplicate must not silently redirect an already-configured namespace elsewhere.
      Map<String, String> parsed =
          PolarisDatasetIdentifier.parseNamespaceCatalogs(List.of("ns=first", "ns=second"));

      assertThat(parsed).containsExactly(Map.entry("ns", "first"));
    }

    @Test
    void anEmptyOrNullConfigYieldsNoMapping() {
      assertThat(PolarisDatasetIdentifier.parseNamespaceCatalogs(List.of())).isEmpty();
      assertThat(PolarisDatasetIdentifier.parseNamespaceCatalogs(null)).isEmpty();
    }

    @Test
    void anUnsetConfigLeavesARecognizedCatalogEndpointAsTheOnlyPolarisSignal() {
      RealmConfig realmConfig = mock(RealmConfig.class);
      when(realmConfig.getConfig(FeatureConfiguration.LINEAGE_NAMESPACE_CATALOGS)).thenReturn(null);

      PolarisDatasetIdentifier unconfigured = PolarisDatasetIdentifier.fromRealmConfig(realmConfig);

      // No mapping, so an unrelated namespace is external however the name is shaped...
      assertThat(unconfigured.identify("s3://warehouse", "sales.orders").kind())
          .isEqualTo(EXTERNAL);
      assertThat(unconfigured.identify("s3://warehouse", "cat.ns.tbl").kind()).isEqualTo(EXTERNAL);
      // ...while a default Polaris catalog endpoint still works with no configuration at all.
      assertThat(unconfigured.identify(POLARIS_CATALOG_URI, "cat.ns.tbl").kind())
          .isEqualTo(POLARIS);
    }

    @Test
    void readsTheRealmConfigKey() {
      RealmConfig realmConfig = mock(RealmConfig.class);
      when(realmConfig.getConfig(FeatureConfiguration.LINEAGE_NAMESPACE_CATALOGS))
          .thenReturn(List.of("s3://warehouse=prod"));

      PolarisDatasetIdentifier configured = PolarisDatasetIdentifier.fromRealmConfig(realmConfig);

      assertThat(configured.identify("s3://warehouse", "ns.tbl").identity())
          .isEqualTo(
              new PolarisDatasetIdentity("prod", TableIdentifier.of(Namespace.of("ns"), "tbl")));
    }
  }
}
