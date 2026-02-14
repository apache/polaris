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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

import java.util.Optional;
import org.apache.polaris.core.persistence.bootstrap.BootstrapOptions;
import org.apache.polaris.core.persistence.bootstrap.SchemaOptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

class JdbcBootstrapUtilsTest {

  @Test
  void getVersion_whenVersionsMatch() {
    // Arrange
    int version = 2;

    // Act & Assert
    assertEquals(
        version, JdbcBootstrapUtils.getRealmBootstrapSchemaVersion(version, version, true));
    assertEquals(
        version, JdbcBootstrapUtils.getRealmBootstrapSchemaVersion(version, version, false));
  }

  @Test
  void getVersion_whenFreshDbAndNoRealms() {
    // Arrange
    int currentVersion = 0;
    boolean hasRealms = false;

    // Act & Assert
    assertEquals(
        3, JdbcBootstrapUtils.getRealmBootstrapSchemaVersion(currentVersion, -1, hasRealms));
    assertEquals(
        2, JdbcBootstrapUtils.getRealmBootstrapSchemaVersion(currentVersion, 2, hasRealms));
    assertEquals(
        3, JdbcBootstrapUtils.getRealmBootstrapSchemaVersion(currentVersion, 3, hasRealms));
  }

  @Test
  void getVersion_whenFreshDbAndRealmsExist() {
    // Arrange
    int currentVersion = 0;
    boolean hasRealms = true;

    // Act & Assert
    assertEquals(
        1, JdbcBootstrapUtils.getRealmBootstrapSchemaVersion(currentVersion, -1, hasRealms));
    assertEquals(
        1, JdbcBootstrapUtils.getRealmBootstrapSchemaVersion(currentVersion, 1, hasRealms));
  }

  @ParameterizedTest
  @CsvSource({"2, true, 2", "3, true, 3", "2, false, 3", "3, false, 3"})
  void getVersion_whenExistingDbAndAutoDetect(
      int currentVersion, boolean hasRealms, int expectedVersion) {
    // Act & Assert
    assertEquals(
        expectedVersion,
        JdbcBootstrapUtils.getRealmBootstrapSchemaVersion(currentVersion, -1, hasRealms));
  }

  @Test
  void throwException_whenFreshDbWithRealmsAndInvalidRequiredVersion() {
    // Arrange
    int currentVersion = 0;
    boolean hasRealms = true;
    int invalidRequiredVersion = 2;

    // Act & Assert
    assertThrows(
        IllegalStateException.class,
        () ->
            JdbcBootstrapUtils.getRealmBootstrapSchemaVersion(
                currentVersion, invalidRequiredVersion, hasRealms));
  }

  @Test
  void throwException_whenExistingDbAndInvalidMigrationPath() {
    // Arrange
    int currentVersion = 2;
    int requiredVersion = 3;

    // Act & Assert
    assertThrows(
        IllegalStateException.class,
        () ->
            JdbcBootstrapUtils.getRealmBootstrapSchemaVersion(
                currentVersion, requiredVersion, true));

    assertThrows(
        IllegalStateException.class,
        () ->
            JdbcBootstrapUtils.getRealmBootstrapSchemaVersion(
                currentVersion, requiredVersion, false));
  }

  @Nested
  @ExtendWith(MockitoExtension.class)
  class GetRequestedSchemaVersionTests {

    @Mock private BootstrapOptions mockBootstrapOptions;
    @Mock private SchemaOptions mockSchemaOptions;

    @BeforeEach
    void setUp() {
      when(mockBootstrapOptions.schemaOptions()).thenReturn(mockSchemaOptions);
    }

    @ParameterizedTest
    @CsvSource({"3", "2", "12"})
    void whenVersionIsInFileName_shouldParseAndReturnIt(int expectedVersion) {
      when(mockSchemaOptions.schemaVersion()).thenReturn(Optional.of(expectedVersion));

      int result = JdbcBootstrapUtils.getRequestedSchemaVersion(mockBootstrapOptions);
      assertEquals(expectedVersion, result);
    }

    @Test
    void whenSchemaOptionsIsNull_shouldReturnDefault() {
      when(mockBootstrapOptions.schemaOptions()).thenReturn(null);
      int result = JdbcBootstrapUtils.getRequestedSchemaVersion(mockBootstrapOptions);
      assertEquals(-1, result);
    }
  }

  @Nested
  @ExtendWith(MockitoExtension.class)
  class ShouldIncludeMetricsTests {

    @Mock private BootstrapOptions mockBootstrapOptions;
    @Mock private SchemaOptions mockSchemaOptions;

    @Test
    void whenSchemaOptionsIsNull_shouldReturnFalse() {
      when(mockBootstrapOptions.schemaOptions()).thenReturn(null);
      boolean result = JdbcBootstrapUtils.shouldIncludeMetrics(mockBootstrapOptions);
      assertEquals(false, result);
    }

    @Test
    void whenIncludeMetricsIsTrue_shouldReturnTrue() {
      when(mockBootstrapOptions.schemaOptions()).thenReturn(mockSchemaOptions);
      when(mockSchemaOptions.includeMetrics()).thenReturn(true);
      boolean result = JdbcBootstrapUtils.shouldIncludeMetrics(mockBootstrapOptions);
      assertEquals(true, result);
    }

    @Test
    void whenIncludeMetricsIsFalse_shouldReturnFalse() {
      when(mockBootstrapOptions.schemaOptions()).thenReturn(mockSchemaOptions);
      when(mockSchemaOptions.includeMetrics()).thenReturn(false);
      boolean result = JdbcBootstrapUtils.shouldIncludeMetrics(mockBootstrapOptions);
      assertEquals(false, result);
    }
  }
}
