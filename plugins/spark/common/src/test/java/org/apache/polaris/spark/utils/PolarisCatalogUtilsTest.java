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
package org.apache.polaris.spark.utils;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

public class PolarisCatalogUtilsTest {

  @Test
  public void testIsTableWithSparkManagedLocationWithNoLocationOrPath() {
    Map<String, String> properties = ImmutableMap.of("key1", "value1", "key2", "value2");

    assertThat(PolarisCatalogUtils.isTableWithSparkManagedLocation(properties)).isTrue();
  }

  @Test
  public void testIsTableWithSparkManagedLocationWithLocation() {
    Map<String, String> properties =
        ImmutableMap.of(TableCatalog.PROP_LOCATION, "s3://bucket/path");

    assertThat(PolarisCatalogUtils.isTableWithSparkManagedLocation(properties)).isFalse();
  }

  @Test
  public void testIsTableWithSparkManagedLocationWithPath() {
    Map<String, String> properties =
        ImmutableMap.of(PolarisCatalogUtils.TABLE_PATH_KEY, "s3://bucket/path");

    assertThat(PolarisCatalogUtils.isTableWithSparkManagedLocation(properties)).isFalse();
  }

  @Test
  public void testIsTableWithSparkManagedLocationWithBothLocationAndPath() {
    Map<String, String> properties =
        ImmutableMap.of(
            TableCatalog.PROP_LOCATION,
            "s3://bucket/location",
            PolarisCatalogUtils.TABLE_PATH_KEY,
            "s3://bucket/path");

    assertThat(PolarisCatalogUtils.isTableWithSparkManagedLocation(properties)).isFalse();
  }

  @Test
  public void testIsTableWithSparkManagedLocationWithEmptyProperties() {
    Map<String, String> properties = ImmutableMap.of();

    assertThat(PolarisCatalogUtils.isTableWithSparkManagedLocation(properties)).isTrue();
  }

  @ParameterizedTest
  @CsvSource({
    "parquet, false, false",
    "csv, false, false",
    "orc, false, false",
    "json, false, false",
    "avro, false, false",
    "delta, false, true",
    "iceberg, true, false",
    "DELTA, false, true",
    "ICEBERG, true, false",
    "DeLta, false, true",
    "IceBerg, true, false"
  })
  public void testProviderDetectionForOtherFormats(
      String provider, boolean expectedIceberg, boolean expectedDelta) {
    assertThat(PolarisCatalogUtils.useIceberg(provider)).isEqualTo(expectedIceberg);
    assertThat(PolarisCatalogUtils.useDelta(provider)).isEqualTo(expectedDelta);
  }
}
