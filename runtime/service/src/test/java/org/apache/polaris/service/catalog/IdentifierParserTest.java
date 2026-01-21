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
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.service.catalog.identifier.CasePreservingNormalizer;
import org.apache.polaris.service.catalog.identifier.IdentifierNormalizer;
import org.apache.polaris.service.catalog.identifier.IdentifierNormalizerFactory;
import org.apache.polaris.service.catalog.identifier.LowercaseNormalizer;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Unit tests for IdentifierParser. */
public class IdentifierParserTest {

  private CatalogCaseSensitivityResolver caseSensitivityResolver;
  private IdentifierNormalizerFactory normalizerFactory;
  private CatalogPrefixParser prefixParser;
  private RealmContext realmContext;
  private IdentifierParser identifierParser;

  @BeforeEach
  public void setUp() {
    caseSensitivityResolver = mock(CatalogCaseSensitivityResolver.class);
    normalizerFactory = new IdentifierNormalizerFactory();
    prefixParser = mock(CatalogPrefixParser.class);
    realmContext = mock(RealmContext.class);

    identifierParser =
        new IdentifierParser(caseSensitivityResolver, normalizerFactory, prefixParser);
  }

  @Test
  public void testParseNamespace_CaseInsensitiveCatalog() {
    // Setup
    String prefix = "test-prefix";
    String namespace = "MyNamespace";
    String catalogName = "test_catalog";

    when(prefixParser.prefixToCatalogName(realmContext, prefix)).thenReturn(catalogName);
    when(caseSensitivityResolver.isCaseInsensitive(catalogName)).thenReturn(true);

    // Test
    Namespace result = identifierParser.parseNamespace(realmContext, prefix, namespace);

    // Verify
    Assertions.assertThat(result.levels())
        .as("Namespace should be lowercased in case-insensitive catalog")
        .containsExactly("mynamespace");

    verify(prefixParser).prefixToCatalogName(realmContext, prefix);
    verify(caseSensitivityResolver).isCaseInsensitive(catalogName);
  }

  @Test
  public void testParseNamespace_CaseSensitiveCatalog() {
    // Setup
    String prefix = "test-prefix";
    String namespace = "MyNamespace";
    String catalogName = "test_catalog";

    when(prefixParser.prefixToCatalogName(realmContext, prefix)).thenReturn(catalogName);
    when(caseSensitivityResolver.isCaseInsensitive(catalogName)).thenReturn(false);

    // Test
    Namespace result = identifierParser.parseNamespace(realmContext, prefix, namespace);

    // Verify
    Assertions.assertThat(result.levels())
        .as("Namespace should preserve case in case-sensitive catalog")
        .containsExactly("MyNamespace");

    verify(prefixParser).prefixToCatalogName(realmContext, prefix);
    verify(caseSensitivityResolver).isCaseInsensitive(catalogName);
  }

  @Test
  public void testParseNamespace_MultiLevel() {
    // Setup
    String prefix = "test-prefix";
    // Multi-level namespace using unit separator character \u001F
    String namespace = "Level1\u001FLevel2\u001FLevel3";
    String catalogName = "test_catalog";

    when(prefixParser.prefixToCatalogName(realmContext, prefix)).thenReturn(catalogName);
    when(caseSensitivityResolver.isCaseInsensitive(catalogName)).thenReturn(true);

    // Test
    Namespace result = identifierParser.parseNamespace(realmContext, prefix, namespace);

    // Verify
    Assertions.assertThat(result.levels())
        .as("Multi-level namespace should be lowercased")
        .containsExactly("level1", "level2", "level3");
  }

  @Test
  public void testParseTableIdentifier_CaseInsensitiveCatalog() {
    // Setup
    String prefix = "test-prefix";
    String namespace = "MyNamespace";
    String table = "MyTable";
    String catalogName = "test_catalog";

    when(prefixParser.prefixToCatalogName(realmContext, prefix)).thenReturn(catalogName);
    when(caseSensitivityResolver.isCaseInsensitive(catalogName)).thenReturn(true);

    // Test
    TableIdentifier result =
        identifierParser.parseTableIdentifier(realmContext, prefix, namespace, table);

    // Verify
    Assertions.assertThat(result.namespace().levels())
        .as("Namespace should be lowercased")
        .containsExactly("mynamespace");
    Assertions.assertThat(result.name())
        .as("Table name should be lowercased")
        .isEqualTo("mytable");
  }

  @Test
  public void testParseTableIdentifier_CaseSensitiveCatalog() {
    // Setup
    String prefix = "test-prefix";
    String namespace = "MyNamespace";
    String table = "MyTable";
    String catalogName = "test_catalog";

    when(prefixParser.prefixToCatalogName(realmContext, prefix)).thenReturn(catalogName);
    when(caseSensitivityResolver.isCaseInsensitive(catalogName)).thenReturn(false);

    // Test
    TableIdentifier result =
        identifierParser.parseTableIdentifier(realmContext, prefix, namespace, table);

    // Verify
    Assertions.assertThat(result.namespace().levels())
        .as("Namespace should preserve case")
        .containsExactly("MyNamespace");
    Assertions.assertThat(result.name())
        .as("Table name should preserve case")
        .isEqualTo("MyTable");
  }

  @Test
  public void testParseTableIdentifier_MultiLevelNamespace() {
    // Setup
    String prefix = "test-prefix";
    // Multi-level namespace using unit separator character \u001F
    String namespace = "DB\u001FSchema";
    String table = "Orders";
    String catalogName = "test_catalog";

    when(prefixParser.prefixToCatalogName(realmContext, prefix)).thenReturn(catalogName);
    when(caseSensitivityResolver.isCaseInsensitive(catalogName)).thenReturn(true);

    // Test
    TableIdentifier result =
        identifierParser.parseTableIdentifier(realmContext, prefix, namespace, table);

    // Verify
    Assertions.assertThat(result.namespace().levels())
        .as("Multi-level namespace should be lowercased")
        .containsExactly("db", "schema");
    Assertions.assertThat(result.name())
        .as("Table name should be lowercased")
        .isEqualTo("orders");
  }

  @Test
  public void testGetNormalizer_CaseInsensitiveCatalog() {
    // Setup
    String prefix = "test-prefix";
    String catalogName = "test_catalog";

    when(prefixParser.prefixToCatalogName(realmContext, prefix)).thenReturn(catalogName);
    when(caseSensitivityResolver.isCaseInsensitive(catalogName)).thenReturn(true);

    // Test
    IdentifierNormalizer normalizer = identifierParser.getNormalizer(realmContext, prefix);

    // Verify
    Assertions.assertThat(normalizer)
        .as("Should return LowercaseNormalizer for case-insensitive catalog")
        .isInstanceOf(LowercaseNormalizer.class);
    Assertions.assertThat(normalizer.isCaseNormalizing())
        .as("Normalizer should be case-normalizing")
        .isTrue();
  }

  @Test
  public void testGetNormalizer_CaseSensitiveCatalog() {
    // Setup
    String prefix = "test-prefix";
    String catalogName = "test_catalog";

    when(prefixParser.prefixToCatalogName(realmContext, prefix)).thenReturn(catalogName);
    when(caseSensitivityResolver.isCaseInsensitive(catalogName)).thenReturn(false);

    // Test
    IdentifierNormalizer normalizer = identifierParser.getNormalizer(realmContext, prefix);

    // Verify
    Assertions.assertThat(normalizer)
        .as("Should return CasePreservingNormalizer for case-sensitive catalog")
        .isInstanceOf(CasePreservingNormalizer.class);
    Assertions.assertThat(normalizer.isCaseNormalizing())
        .as("Normalizer should not be case-normalizing")
        .isFalse();
  }

  @Test
  public void testIsCaseInsensitive_True() {
    // Setup
    String prefix = "test-prefix";
    String catalogName = "test_catalog";

    when(prefixParser.prefixToCatalogName(realmContext, prefix)).thenReturn(catalogName);
    when(caseSensitivityResolver.isCaseInsensitive(catalogName)).thenReturn(true);

    // Test
    boolean result = identifierParser.isCaseInsensitive(realmContext, prefix);

    // Verify
    Assertions.assertThat(result)
        .as("Should return true for case-insensitive catalog")
        .isTrue();
  }

  @Test
  public void testIsCaseInsensitive_False() {
    // Setup
    String prefix = "test-prefix";
    String catalogName = "test_catalog";

    when(prefixParser.prefixToCatalogName(realmContext, prefix)).thenReturn(catalogName);
    when(caseSensitivityResolver.isCaseInsensitive(catalogName)).thenReturn(false);

    // Test
    boolean result = identifierParser.isCaseInsensitive(realmContext, prefix);

    // Verify
    Assertions.assertThat(result)
        .as("Should return false for case-sensitive catalog")
        .isFalse();
  }

  @Test
  public void testNormalizeTableIdentifier_CaseInsensitive() {
    // Setup
    String prefix = "test-prefix";
    String catalogName = "test_catalog";
    TableIdentifier identifier = TableIdentifier.of(Namespace.of("MyDB"), "MyTable");

    when(prefixParser.prefixToCatalogName(realmContext, prefix)).thenReturn(catalogName);
    when(caseSensitivityResolver.isCaseInsensitive(catalogName)).thenReturn(true);

    // Test
    TableIdentifier result =
        identifierParser.normalizeTableIdentifier(realmContext, prefix, identifier);

    // Verify
    Assertions.assertThat(result.namespace().levels())
        .as("Namespace should be lowercased")
        .containsExactly("mydb");
    Assertions.assertThat(result.name())
        .as("Table name should be lowercased")
        .isEqualTo("mytable");
  }

  @Test
  public void testNormalizeTableIdentifier_CaseSensitive() {
    // Setup
    String prefix = "test-prefix";
    String catalogName = "test_catalog";
    TableIdentifier identifier = TableIdentifier.of(Namespace.of("MyDB"), "MyTable");

    when(prefixParser.prefixToCatalogName(realmContext, prefix)).thenReturn(catalogName);
    when(caseSensitivityResolver.isCaseInsensitive(catalogName)).thenReturn(false);

    // Test
    TableIdentifier result =
        identifierParser.normalizeTableIdentifier(realmContext, prefix, identifier);

    // Verify
    Assertions.assertThat(result.namespace().levels())
        .as("Namespace should preserve case")
        .containsExactly("MyDB");
    Assertions.assertThat(result.name())
        .as("Table name should preserve case")
        .isEqualTo("MyTable");
  }

  @Test
  public void testNormalizeTableIdentifier_Null() {
    // Setup
    String prefix = "test-prefix";
    String catalogName = "test_catalog";

    when(prefixParser.prefixToCatalogName(realmContext, prefix)).thenReturn(catalogName);
    when(caseSensitivityResolver.isCaseInsensitive(catalogName)).thenReturn(true);

    // Test
    TableIdentifier result = identifierParser.normalizeTableIdentifier(realmContext, prefix, null);

    // Verify
    Assertions.assertThat(result)
        .as("Null identifier should return null")
        .isNull();
  }

  @Test
  public void testNormalizeNamespace_CaseInsensitive() {
    // Setup
    String prefix = "test-prefix";
    String catalogName = "test_catalog";
    Namespace namespace = Namespace.of("MyDB", "MySchema");

    when(prefixParser.prefixToCatalogName(realmContext, prefix)).thenReturn(catalogName);
    when(caseSensitivityResolver.isCaseInsensitive(catalogName)).thenReturn(true);

    // Test
    Namespace result = identifierParser.normalizeNamespace(realmContext, prefix, namespace);

    // Verify
    Assertions.assertThat(result.levels())
        .as("Namespace should be lowercased")
        .containsExactly("mydb", "myschema");
  }

  @Test
  public void testNormalizeNamespace_CaseSensitive() {
    // Setup
    String prefix = "test-prefix";
    String catalogName = "test_catalog";
    Namespace namespace = Namespace.of("MyDB", "MySchema");

    when(prefixParser.prefixToCatalogName(realmContext, prefix)).thenReturn(catalogName);
    when(caseSensitivityResolver.isCaseInsensitive(catalogName)).thenReturn(false);

    // Test
    Namespace result = identifierParser.normalizeNamespace(realmContext, prefix, namespace);

    // Verify
    Assertions.assertThat(result.levels())
        .as("Namespace should preserve case")
        .containsExactly("MyDB", "MySchema");
  }

  @Test
  public void testNormalizeNamespace_Null() {
    // Setup
    String prefix = "test-prefix";
    String catalogName = "test_catalog";

    when(prefixParser.prefixToCatalogName(realmContext, prefix)).thenReturn(catalogName);
    when(caseSensitivityResolver.isCaseInsensitive(catalogName)).thenReturn(true);

    // Test
    Namespace result = identifierParser.normalizeNamespace(realmContext, prefix, null);

    // Verify
    Assertions.assertThat(result)
        .as("Null namespace should return null")
        .isNull();
  }

  @Test
  public void testDecodeNamespace() {
    // Test the utility method that decodes without normalization
    String encoded = "MyNamespace";

    Namespace result = identifierParser.decodeNamespace(encoded);

    Assertions.assertThat(result.levels())
        .as("Decoded namespace should preserve case (no normalization)")
        .containsExactly("MyNamespace");
  }

  @Test
  public void testSpecialCharactersInIdentifiers() {
    // Setup
    String prefix = "test-prefix";
    String namespace = "my_namespace";
    String table = "my-table-2024";
    String catalogName = "test_catalog";

    when(prefixParser.prefixToCatalogName(realmContext, prefix)).thenReturn(catalogName);
    when(caseSensitivityResolver.isCaseInsensitive(catalogName)).thenReturn(true);

    // Test
    TableIdentifier result =
        identifierParser.parseTableIdentifier(realmContext, prefix, namespace, table);

    // Verify special characters are preserved
    Assertions.assertThat(result.namespace().levels())
        .as("Underscores should be preserved")
        .containsExactly("my_namespace");
    Assertions.assertThat(result.name())
        .as("Hyphens and numbers should be preserved")
        .isEqualTo("my-table-2024");
  }
}
