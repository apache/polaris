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
package org.apache.polaris.service.catalog.identifier;

import java.util.Arrays;
import java.util.List;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class LowercaseNormalizerTest {

  private final LowercaseNormalizer normalizer = LowercaseNormalizer.INSTANCE;

  @Test
  public void testNormalizeNameLowercase() {
    Assertions.assertThat(normalizer.normalizeName("test")).isEqualTo("test");
    Assertions.assertThat(normalizer.normalizeName("TEST")).isEqualTo("test");
    Assertions.assertThat(normalizer.normalizeName("Test")).isEqualTo("test");
    Assertions.assertThat(normalizer.normalizeName("TeSt123")).isEqualTo("test123");
  }

  @Test
  public void testNormalizeNameNull() {
    Assertions.assertThat(normalizer.normalizeName(null)).isNull();
  }

  @Test
  public void testNormalizeNameMixedCase() {
    Assertions.assertThat(normalizer.normalizeName("MyDatabase")).isEqualTo("mydatabase");
    Assertions.assertThat(normalizer.normalizeName("my_table")).isEqualTo("my_table");
    Assertions.assertThat(normalizer.normalizeName("MY_TABLE")).isEqualTo("my_table");
  }

  @Test
  public void testNormalizeNameWithSpecialCharacters() {
    Assertions.assertThat(normalizer.normalizeName("test-name")).isEqualTo("test-name");
    Assertions.assertThat(normalizer.normalizeName("test_name")).isEqualTo("test_name");
    Assertions.assertThat(normalizer.normalizeName("test.name")).isEqualTo("test.name");
  }

  @Test
  public void testNormalizeNamespaceEmpty() {
    Namespace empty = Namespace.empty();
    Assertions.assertThat(normalizer.normalizeNamespace(empty)).isEqualTo(empty);
  }

  @Test
  public void testNormalizeNamespaceNull() {
    Assertions.assertThat(normalizer.normalizeNamespace(null)).isNull();
  }

  @Test
  public void testNormalizeNamespaceSingleLevel() {
    Namespace ns = Namespace.of("MyDatabase");
    Namespace normalized = normalizer.normalizeNamespace(ns);
    Assertions.assertThat(normalized.levels()).containsExactly("mydatabase");
  }

  @Test
  public void testNormalizeNamespaceMultipleLevels() {
    Namespace ns = Namespace.of("MyDatabase", "MySchema");
    Namespace normalized = normalizer.normalizeNamespace(ns);
    Assertions.assertThat(normalized.levels()).containsExactly("mydatabase", "myschema");
  }

  @Test
  public void testNormalizeTableIdentifierNull() {
    Assertions.assertThat(normalizer.normalizeTableIdentifier(null)).isNull();
  }

  @Test
  public void testNormalizeTableIdentifierSimple() {
    TableIdentifier id = TableIdentifier.of("MyNamespace", "MyTable");
    TableIdentifier normalized = normalizer.normalizeTableIdentifier(id);
    Assertions.assertThat(normalized.namespace().levels()).containsExactly("mynamespace");
    Assertions.assertThat(normalized.name()).isEqualTo("mytable");
  }

  @Test
  public void testNormalizeTableIdentifierNestedNamespace() {
    TableIdentifier id = TableIdentifier.of(Namespace.of("DB", "Schema"), "Table");
    TableIdentifier normalized = normalizer.normalizeTableIdentifier(id);
    Assertions.assertThat(normalized.namespace().levels()).containsExactly("db", "schema");
    Assertions.assertThat(normalized.name()).isEqualTo("table");
  }

  @Test
  public void testNormalizeNamesNull() {
    Assertions.assertThat(normalizer.normalizeNames(null)).isNull();
  }

  @Test
  public void testNormalizeNamesList() {
    List<String> names = Arrays.asList("Foo", "BAR", "baz");
    List<String> normalized = normalizer.normalizeNames(names);
    Assertions.assertThat(normalized).containsExactly("foo", "bar", "baz");
  }

  @Test
  public void testIsCaseNormalizing() {
    Assertions.assertThat(normalizer.isCaseNormalizing()).isTrue();
  }

  @Test
  public void testLocaleIndependence() {
    // Test that Turkish İ/I case conversion is handled consistently with Locale.ROOT
    // Locale.ROOT ensures 'I' lowercases to 'i', not dotless 'ı'
    Assertions.assertThat(normalizer.normalizeName("ITEM")).isEqualTo("item");
  }

  // ========================================================================
  // Internationalization (i18n) Tests
  // These tests document the ACTUAL behavior of Locale.ROOT with special
  // characters from Turkish, German, and Greek alphabets.
  // ========================================================================

  /**
   * Turkish dotless i (ı, U+0131) is NOT converted to ASCII 'i' by Locale.ROOT.
   *
   * <p>In Turkish: - Uppercase 'I' should lowercase to 'ı' (dotless i) - Uppercase 'İ' (U+0130)
   * should lowercase to 'i' (dotted i)
   *
   * <p>However, Locale.ROOT uses ASCII-based rules: - 'I' → 'i' (ASCII behavior, correct) - 'ı'
   * (U+0131) → 'ı' (unchanged, stays as dotless i) - 'İ' (U+0130) → 'i̇' (capital I with dot →
   * lowercase i with combining dot above)
   *
   * <p>This means Turkish identifiers with 'ı' will not match identifiers with 'I', which may be
   * unexpected for Turkish users.
   */
  @Test
  public void testTurkishDotlessI() {
    // Turkish lowercase dotless i (U+0131) stays unchanged
    // This character appears in Turkish words like "tabları" (tables)
    String withDotlessI = "tabLARı";
    Assertions.assertThat(normalizer.normalizeName(withDotlessI))
        .as("Turkish dotless i (ı) should remain unchanged with Locale.ROOT")
        .isEqualTo("tabları");

    // Regular ASCII 'I' correctly lowercases to 'i'
    String asciiI = "tabLARI";
    Assertions.assertThat(normalizer.normalizeName(asciiI))
        .as("ASCII 'I' should lowercase to 'i'")
        .isEqualTo("tablari");

    // These two are NOT equal, demonstrating the limitation
    Assertions.assertThat(normalizer.normalizeName("tabLARı"))
        .as("Turkish 'ı' and ASCII 'i' produce different normalized forms")
        .isNotEqualTo(normalizer.normalizeName("tabLARI"));
  }

  /**
   * Turkish capital I with dot (İ, U+0130) behavior with Locale.ROOT.
   *
   * <p>In Turkish, 'İ' (capital I with dot) should lowercase to regular 'i'. With Locale.ROOT, it
   * may produce unexpected results depending on the Java version and Unicode normalization.
   *
   * <p>Common cities/identifiers: İSTANBUL, İZMİR
   */
  @Test
  public void testTurkishCapitalIWithDot() {
    // Turkish capital I with dot (U+0130)
    // Cities like "İstanbul" use this character
    String istanbul = "İSTANBUL";

    String normalized = normalizer.normalizeName(istanbul);

    // With Locale.ROOT, İ (U+0130) typically lowercases to i with combining dot above
    // The exact behavior may vary, but we document what actually happens
    Assertions.assertThat(normalized)
        .as(
            "Turkish capital İ (U+0130) with Locale.ROOT - documenting actual behavior: %s",
            normalized)
        .isNotNull();

    // This demonstrates that İSTANBUL and ISTANBUL produce different results
    Assertions.assertThat(normalizer.normalizeName("İSTANBUL"))
        .as("Turkish İ and ASCII I produce different normalized forms")
        .isNotEqualTo(normalizer.normalizeName("ISTANBUL"));
  }

  /**
   * German eszett (ß, U+00DF) behavior with Locale.ROOT.
   *
   * <p>The German eszett (ß) has asymmetric case conversion: - Lowercase: ß → ß (unchanged) -
   * Uppercase: ß → SS (becomes two characters)
   *
   * <p>This means "Straße" and "Strasse" cannot be unified through case normalization. Locale.ROOT
   * keeps ß as lowercase ß.
   */
  @Test
  public void testGermanEszett() {
    // German word "Straße" (street) with eszett
    String strasse = "Straße";
    Assertions.assertThat(normalizer.normalizeName(strasse))
        .as("German ß should remain lowercase ß with Locale.ROOT")
        .isEqualTo("straße");

    // Alternative spelling without eszett
    String strasseWithSS = "Strasse";
    Assertions.assertThat(normalizer.normalizeName(strasseWithSS))
        .as("'ss' should remain lowercase 'ss'")
        .isEqualTo("strasse");

    // These are NOT equal - ß and ss are different characters
    Assertions.assertThat(normalizer.normalizeName("Straße"))
        .as("German 'ß' and 'ss' produce different normalized forms")
        .isNotEqualTo(normalizer.normalizeName("Strasse"));
  }

  /**
   * German eszett uppercased first then lowercased behavior.
   *
   * <p>If an identifier is uppercased before being lowercased, eszett undergoes lossy conversion:
   * ß → SS → ss
   *
   * <p>This demonstrates why case normalization cannot unify all variants.
   */
  @Test
  public void testGermanEszettUppercasedFirst() {
    // If someone uppercases "Straße" first, it becomes "STRASSE"
    String uppercased = "Straße".toUpperCase(java.util.Locale.ROOT);
    Assertions.assertThat(uppercased)
        .as("Uppercasing ß with Locale.ROOT produces 'SS'")
        .isEqualTo("STRASSE");

    // Then lowercasing produces "strasse" (with ss, not ß)
    Assertions.assertThat(normalizer.normalizeName(uppercased))
        .as("Lowercasing 'STRASSE' produces 'strasse' with 'ss'")
        .isEqualTo("strasse");

    // This is different from directly lowercasing "Straße"
    Assertions.assertThat(normalizer.normalizeName("Straße"))
        .as("Direct lowercase preserves ß, but uppercase→lowercase converts to ss")
        .isNotEqualTo(normalizer.normalizeName(uppercased));
  }

  /**
   * Greek capital sigma (Σ, U+03A3) behavior with Locale.ROOT.
   *
   * <p>Greek has two lowercase sigma forms: - σ (U+03C3): regular lowercase sigma, used in middle
   * of words - ς (U+03C2): final sigma, used at end of words
   *
   * <p>Locale.ROOT lowercases Σ to σ (regular sigma), not considering word position.
   */
  @Test
  public void testGreekCapitalSigma() {
    // Greek word "ΣΧΗΜΑ" (schema/shape) with capital sigma
    String schema = "ΣΧΗΜΑ";
    Assertions.assertThat(normalizer.normalizeName(schema))
        .as("Greek capital Σ should lowercase to σ with Locale.ROOT")
        .isEqualTo("σχημα");

    // Capital sigma at different positions
    String sigmaStart = "ΣΗΜΑ";
    Assertions.assertThat(normalizer.normalizeName(sigmaStart))
        .as("Capital Σ at start lowercases to σ")
        .contains("σ");
  }

  /**
   * Greek final sigma (ς, U+03C2) behavior with Locale.ROOT.
   *
   * <p>The final sigma (ς) is used at the end of words in Greek. It stays as-is when already
   * lowercase. Regular sigma (σ) and final sigma (ς) are different characters and won't match.
   */
  @Test
  public void testGreekFinalSigma() {
    // Greek word "τέλος" (end) with final sigma
    String telos = "τέλος";
    Assertions.assertThat(normalizer.normalizeName(telos))
        .as("Final sigma ς should remain unchanged")
        .isEqualTo("τέλος");

    // If someone uses regular sigma instead of final sigma
    String telosRegular = "τέλοσ"; // using σ instead of ς
    Assertions.assertThat(normalizer.normalizeName(telosRegular))
        .as("Regular sigma σ should remain as σ")
        .isEqualTo("τέλοσ");

    // These are NOT equal - different sigma forms
    Assertions.assertThat(normalizer.normalizeName("τέλος"))
        .as("Final sigma ς and regular sigma σ produce different results")
        .isNotEqualTo(normalizer.normalizeName("τέλοσ"));
  }

  /**
   * Namespace and TableIdentifier normalization with i18n characters.
   *
   * <p>This test verifies that i18n characters are preserved through namespace and table identifier
   * normalization, maintaining the same limitations documented above.
   */
  @Test
  public void testInternationalCharactersInIdentifiers() {
    // Turkish namespace
    Namespace turkishNs = Namespace.of("Şİrket", "Veritabanı");
    Namespace normalizedTurkishNs = normalizer.normalizeNamespace(turkishNs);
    Assertions.assertThat(normalizedTurkishNs.levels())
        .as("Turkish characters in namespace should be normalized")
        .hasSize(2);

    // German table identifier
    TableIdentifier germanTable = TableIdentifier.of("Datenbank", "Straße_Tabelle");
    TableIdentifier normalizedGerman = normalizer.normalizeTableIdentifier(germanTable);
    Assertions.assertThat(normalizedGerman.name())
        .as("German ß should remain in normalized table name")
        .contains("ß");

    // Greek table identifier
    TableIdentifier greekTable = TableIdentifier.of(Namespace.of("ΣΧΗΜΑ"), "ΠΙΝΑΚΑΣ");
    TableIdentifier normalizedGreek = normalizer.normalizeTableIdentifier(greekTable);
    Assertions.assertThat(normalizedGreek.namespace().levels())
        .as("Greek capital letters should be lowercased")
        .containsExactly("σχημα");
    Assertions.assertThat(normalizedGreek.name())
        .as("Greek table name should be lowercased")
        .isEqualTo("πινακας");
  }

  /**
   * Combined test showing that ASCII identifiers work correctly regardless of i18n limitations.
   *
   * <p>This demonstrates that the common case (ASCII a-z, 0-9, underscore) works perfectly with
   * Locale.ROOT, which is why this approach is acceptable for most data lake use cases.
   */
  @Test
  public void testAsciiIdentifiersWorkCorrectly() {
    // Common ASCII identifier patterns
    Assertions.assertThat(normalizer.normalizeName("MY_DATABASE")).isEqualTo("my_database");
    Assertions.assertThat(normalizer.normalizeName("Employee123")).isEqualTo("employee123");
    Assertions.assertThat(normalizer.normalizeName("SALES_DATA_2024")).isEqualTo("sales_data_2024");

    // Namespace with ASCII characters
    Namespace asciiNs = Namespace.of("PROD", "Sales", "NA");
    Namespace normalized = normalizer.normalizeNamespace(asciiNs);
    Assertions.assertThat(normalized.levels()).containsExactly("prod", "sales", "na");

    // TableIdentifier with ASCII characters
    TableIdentifier asciiTable = TableIdentifier.of(Namespace.of("PROD", "Sales"), "ORDERS_2024");
    TableIdentifier normalizedTable = normalizer.normalizeTableIdentifier(asciiTable);
    Assertions.assertThat(normalizedTable.namespace().levels()).containsExactly("prod", "sales");
    Assertions.assertThat(normalizedTable.name()).isEqualTo("orders_2024");
  }
}
