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
package org.apache.polaris.service.catalog;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.polaris.core.entity.CatalogEntity;
import org.apache.polaris.core.persistence.resolver.PolarisResolutionManifest;
import org.apache.polaris.core.persistence.resolver.ResolutionManifestFactory;
import org.apache.polaris.service.catalog.identifier.CasePreservingNormalizer;
import org.apache.polaris.service.catalog.identifier.IdentifierNormalizer;
import org.apache.polaris.service.catalog.identifier.IdentifierNormalizerFactory;
import org.apache.polaris.service.catalog.identifier.LowercaseNormalizer;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class IdentifierParserFactoryTest {

  private IdentifierNormalizerFactory normalizerFactory;
  private ResolutionManifestFactory resolutionManifestFactory;
  private IdentifierParserFactory parserFactory;
  private LowercaseNormalizer lowercaseNormalizer;
  private CasePreservingNormalizer casePreservingNormalizer;

  @BeforeEach
  public void setUp() {
    normalizerFactory = mock(IdentifierNormalizerFactory.class);
    resolutionManifestFactory = mock(ResolutionManifestFactory.class);
    lowercaseNormalizer = LowercaseNormalizer.INSTANCE;
    casePreservingNormalizer = CasePreservingNormalizer.INSTANCE;

    when(normalizerFactory.getCaseInsensitiveNormalizer()).thenReturn(lowercaseNormalizer);
    when(normalizerFactory.getDefaultNormalizer()).thenReturn(casePreservingNormalizer);

    parserFactory = new IdentifierParserFactory(normalizerFactory, resolutionManifestFactory);
  }

  @Test
  public void testCreateParser_CaseInsensitiveCatalog() {
    // Create a case-insensitive catalog
    CatalogEntity catalog =
        new CatalogEntity.Builder()
            .setName("test_catalog")
            .setCaseInsensitiveMode(true)
            .build();

    // Mock manifest to return case-insensitive catalog
    PolarisResolutionManifest manifest = mock(PolarisResolutionManifest.class);
    when(manifest.getResolvedCatalogEntity()).thenReturn(catalog);

    // Create parser
    IdentifierParser parser = parserFactory.createParser(manifest);

    // Verify parser is created and uses lowercase normalizer
    Assertions.assertThat(parser).isNotNull();
    verify(normalizerFactory, times(1)).getCaseInsensitiveNormalizer();

    // Verify parser behavior - should lowercase
    Assertions.assertThat(parser.parseNamespace("MyNamespace"))
        .isEqualTo(org.apache.iceberg.catalog.Namespace.of("mynamespace"));
  }

  @Test
  public void testCreateParser_CaseSensitiveCatalog() {
    // Create a case-sensitive catalog
    CatalogEntity catalog =
        new CatalogEntity.Builder()
            .setName("test_catalog")
            .setCaseInsensitiveMode(false)
            .build();

    // Mock manifest to return case-sensitive catalog
    PolarisResolutionManifest manifest = mock(PolarisResolutionManifest.class);
    when(manifest.getResolvedCatalogEntity()).thenReturn(catalog);

    // Create parser
    IdentifierParser parser = parserFactory.createParser(manifest);

    // Verify parser is created and uses case-preserving normalizer
    Assertions.assertThat(parser).isNotNull();
    verify(normalizerFactory, times(1)).getDefaultNormalizer();

    // Verify parser behavior - should preserve case
    Assertions.assertThat(parser.parseNamespace("MyNamespace"))
        .isEqualTo(org.apache.iceberg.catalog.Namespace.of("MyNamespace"));
  }

  @Test
  public void testCreateParser_NullCatalog() {
    // Mock manifest to return null catalog
    PolarisResolutionManifest manifest = mock(PolarisResolutionManifest.class);
    when(manifest.getResolvedCatalogEntity()).thenReturn(null);

    // Create parser - should default to case-sensitive
    IdentifierParser parser = parserFactory.createParser(manifest);

    // Verify parser is created and uses case-preserving normalizer (default)
    Assertions.assertThat(parser).isNotNull();
    verify(normalizerFactory, times(1)).getDefaultNormalizer();

    // Verify parser behavior - should preserve case
    Assertions.assertThat(parser.parseNamespace("MyNamespace"))
        .isEqualTo(org.apache.iceberg.catalog.Namespace.of("MyNamespace"));
  }

  @Test
  public void testCreateParser_CatalogWithoutExplicitSetting() {
    // Create catalog without explicit case-insensitive setting (defaults to false)
    CatalogEntity catalog = new CatalogEntity.Builder().setName("test_catalog").build();

    // Mock manifest to return catalog
    PolarisResolutionManifest manifest = mock(PolarisResolutionManifest.class);
    when(manifest.getResolvedCatalogEntity()).thenReturn(catalog);

    // Create parser - should default to case-sensitive
    IdentifierParser parser = parserFactory.createParser(manifest);

    // Verify parser uses case-preserving normalizer
    Assertions.assertThat(parser).isNotNull();
    verify(normalizerFactory, times(1)).getDefaultNormalizer();

    // Verify parser behavior - should preserve case
    Assertions.assertThat(parser.parseNamespace("MyNamespace"))
        .isEqualTo(org.apache.iceberg.catalog.Namespace.of("MyNamespace"));
  }

  @Test
  public void testIsCaseInsensitive_CaseInsensitiveCatalog() {
    // Create a case-insensitive catalog
    CatalogEntity catalog =
        new CatalogEntity.Builder()
            .setName("test_catalog")
            .setCaseInsensitiveMode(true)
            .build();

    // Mock manifest
    PolarisResolutionManifest manifest = mock(PolarisResolutionManifest.class);
    when(manifest.getResolvedCatalogEntity()).thenReturn(catalog);

    // Test
    boolean isCaseInsensitive = parserFactory.isCaseInsensitive(manifest);

    Assertions.assertThat(isCaseInsensitive)
        .as("Case-insensitive catalog should return true")
        .isTrue();
  }

  @Test
  public void testIsCaseInsensitive_CaseSensitiveCatalog() {
    // Create a case-sensitive catalog
    CatalogEntity catalog =
        new CatalogEntity.Builder()
            .setName("test_catalog")
            .setCaseInsensitiveMode(false)
            .build();

    // Mock manifest
    PolarisResolutionManifest manifest = mock(PolarisResolutionManifest.class);
    when(manifest.getResolvedCatalogEntity()).thenReturn(catalog);

    // Test
    boolean isCaseInsensitive = parserFactory.isCaseInsensitive(manifest);

    Assertions.assertThat(isCaseInsensitive)
        .as("Case-sensitive catalog should return false")
        .isFalse();
  }

  @Test
  public void testIsCaseInsensitive_NullCatalog() {
    // Mock manifest to return null catalog
    PolarisResolutionManifest manifest = mock(PolarisResolutionManifest.class);
    when(manifest.getResolvedCatalogEntity()).thenReturn(null);

    // Test - should default to case-sensitive
    boolean isCaseInsensitive = parserFactory.isCaseInsensitive(manifest);

    Assertions.assertThat(isCaseInsensitive)
        .as("Null catalog should default to case-sensitive (false)")
        .isFalse();
  }

  @Test
  public void testCreateParser_MultipleParsersFromSameManifest() {
    // Create a case-insensitive catalog
    CatalogEntity catalog =
        new CatalogEntity.Builder()
            .setName("test_catalog")
            .setCaseInsensitiveMode(true)
            .build();

    // Mock manifest
    PolarisResolutionManifest manifest = mock(PolarisResolutionManifest.class);
    when(manifest.getResolvedCatalogEntity()).thenReturn(catalog);

    // Create multiple parsers
    IdentifierParser parser1 = parserFactory.createParser(manifest);
    IdentifierParser parser2 = parserFactory.createParser(manifest);

    // Both should be valid and independent instances
    Assertions.assertThat(parser1).isNotNull();
    Assertions.assertThat(parser2).isNotNull();
    Assertions.assertThat(parser1).isNotSameAs(parser2);

    // Both should normalize identically
    Assertions.assertThat(parser1.parseNamespace("MyNamespace"))
        .isEqualTo(parser2.parseNamespace("MyNamespace"));

    // Verify normalizer was retrieved twice
    verify(normalizerFactory, times(2)).getCaseInsensitiveNormalizer();
  }

  @Test
  public void testCreateParser_DifferentManifests() {
    // Create two catalogs with different case-sensitivity settings
    CatalogEntity caseInsensitiveCatalog =
        new CatalogEntity.Builder()
            .setName("case_insensitive_catalog")
            .setCaseInsensitiveMode(true)
            .build();

    CatalogEntity caseSensitiveCatalog =
        new CatalogEntity.Builder()
            .setName("case_sensitive_catalog")
            .setCaseInsensitiveMode(false)
            .build();

    // Mock manifests
    PolarisResolutionManifest manifest1 = mock(PolarisResolutionManifest.class);
    when(manifest1.getResolvedCatalogEntity()).thenReturn(caseInsensitiveCatalog);

    PolarisResolutionManifest manifest2 = mock(PolarisResolutionManifest.class);
    when(manifest2.getResolvedCatalogEntity()).thenReturn(caseSensitiveCatalog);

    // Create parsers
    IdentifierParser parser1 = parserFactory.createParser(manifest1);
    IdentifierParser parser2 = parserFactory.createParser(manifest2);

    // Verify different behavior
    Assertions.assertThat(parser1.parseNamespace("MyNamespace"))
        .isEqualTo(org.apache.iceberg.catalog.Namespace.of("mynamespace"));

    Assertions.assertThat(parser2.parseNamespace("MyNamespace"))
        .isEqualTo(org.apache.iceberg.catalog.Namespace.of("MyNamespace"));

    // Verify correct normalizers were requested
    verify(normalizerFactory, times(1)).getCaseInsensitiveNormalizer();
    verify(normalizerFactory, times(1)).getDefaultNormalizer();
  }

  @Test
  public void testCreateParser_TableIdentifierNormalization() {
    // Create a case-insensitive catalog
    CatalogEntity catalog =
        new CatalogEntity.Builder()
            .setName("test_catalog")
            .setCaseInsensitiveMode(true)
            .build();

    // Mock manifest
    PolarisResolutionManifest manifest = mock(PolarisResolutionManifest.class);
    when(manifest.getResolvedCatalogEntity()).thenReturn(catalog);

    // Create parser
    IdentifierParser parser = parserFactory.createParser(manifest);

    // Test table identifier parsing
    org.apache.iceberg.catalog.TableIdentifier table =
        parser.parseTableIdentifier("MyNamespace", "MyTable");

    Assertions.assertThat(table.namespace())
        .isEqualTo(org.apache.iceberg.catalog.Namespace.of("mynamespace"));
    Assertions.assertThat(table.name()).isEqualTo("mytable");
  }

  @Test
  public void testCreateParser_NormalizerFactoryIntegration() {
    IdentifierNormalizerFactory realFactory = new IdentifierNormalizerFactory();
    ResolutionManifestFactory mockManifestFactory = mock(ResolutionManifestFactory.class);
    IdentifierParserFactory realParserFactory =
        new IdentifierParserFactory(realFactory, mockManifestFactory);

    // Create case-insensitive catalog
    CatalogEntity catalog =
        new CatalogEntity.Builder()
            .setName("test_catalog")
            .setCaseInsensitiveMode(true)
            .build();

    PolarisResolutionManifest manifest = mock(PolarisResolutionManifest.class);
    when(manifest.getResolvedCatalogEntity()).thenReturn(catalog);

    // Create parser with real factory
    IdentifierParser parser = realParserFactory.createParser(manifest);

    // Verify it works end-to-end
    Assertions.assertThat(parser.parseNamespace("TEST"))
        .isEqualTo(org.apache.iceberg.catalog.Namespace.of("test"));
  }
}
