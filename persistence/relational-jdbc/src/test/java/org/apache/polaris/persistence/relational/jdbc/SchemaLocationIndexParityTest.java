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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

/**
 * Verifies that the {@code idx_locations} index uses the same column list ({@code realm_id,
 * catalog_id, location_without_scheme}) across all database backends. The optimized sibling
 * check reads this index via {@link QueryGenerator#generateOverlapQuery}, which filters on
 * {@code realm_id} and {@code catalog_id}. If the index leads with {@code parent_id} instead
 * of {@code catalog_id}, the query can only use the {@code realm_id} prefix and falls back to
 * a realm-wide scan.
 */
class SchemaLocationIndexParityTest {

  private static final Pattern IDX_LOCATIONS_PATTERN =
      Pattern.compile(
          "CREATE\\s+INDEX.*?idx_locations.*?\\(\\s*([^)]+)\\s*\\)",
          Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

  @Test
  void idxLocationsColumnsMatchAcrossBackends() {
    List<String> expectedColumns = List.of("realm_id", "catalog_id", "location_without_scheme");

    for (DatabaseType dbType : DatabaseType.values()) {
      int latestVersion = dbType.getLatestSchemaVersion();
      String resourcePath =
          String.format("%s/schema-v%d.sql", dbType.getDisplayName(), latestVersion);

      InputStream stream =
          SchemaLocationIndexParityTest.class.getClassLoader().getResourceAsStream(resourcePath);
      assertThat(stream)
          .as("Schema resource %s must exist", resourcePath)
          .isNotNull();

      String sql;
      try (BufferedReader reader =
          new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {
        sql = reader.lines().collect(Collectors.joining("\n"));
      } catch (Exception e) {
        throw new RuntimeException("Failed to read " + resourcePath, e);
      }

      Matcher matcher = IDX_LOCATIONS_PATTERN.matcher(sql);
      assertThat(matcher.find())
          .as("Schema %s must contain an idx_locations index definition", resourcePath)
          .isTrue();

      String columnsPart = matcher.group(1);
      List<String> actualColumns = new ArrayList<>();
      for (String col : columnsPart.split(",")) {
        actualColumns.add(col.trim().toLowerCase());
      }

      assertThat(actualColumns)
          .as(
              "idx_locations columns in %s must match the optimized sibling-check query predicate",
              resourcePath)
          .containsExactlyElementsOf(expectedColumns);
    }
  }
}