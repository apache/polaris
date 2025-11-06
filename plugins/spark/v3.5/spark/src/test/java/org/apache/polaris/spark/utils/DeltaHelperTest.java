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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

import com.google.common.collect.ImmutableMap;
import org.apache.polaris.spark.NoopDeltaCatalog;
import org.apache.polaris.spark.PolarisSparkCatalog;
import org.apache.spark.sql.connector.catalog.DelegatingCatalogExtension;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.Test;

public class DeltaHelperTest {

  @Test
  public void testConstructorWithCustomDeltaCatalogImpl() {
    CaseInsensitiveStringMap options =
        new CaseInsensitiveStringMap(
            ImmutableMap.of(
                DeltaHelper.DELTA_CATALOG_IMPL_KEY, "org.apache.polaris.spark.NoopDeltaCatalog"));
    DeltaHelper helper = new DeltaHelper(options);

    assertThat(helper).isNotNull();
  }

  @Test
  public void testLoadDeltaCatalogWithNoopDeltaCatalog() {
    CaseInsensitiveStringMap options =
        new CaseInsensitiveStringMap(
            ImmutableMap.of(
                DeltaHelper.DELTA_CATALOG_IMPL_KEY, "org.apache.polaris.spark.NoopDeltaCatalog"));
    DeltaHelper helper = new DeltaHelper(options);
    PolarisSparkCatalog polarisSparkCatalog = mock(PolarisSparkCatalog.class);

    TableCatalog deltaCatalog = helper.loadDeltaCatalog(polarisSparkCatalog);

    assertThat(deltaCatalog).isNotNull();
    assertThat(deltaCatalog).isInstanceOf(NoopDeltaCatalog.class);
    assertThat(deltaCatalog).isInstanceOf(DelegatingCatalogExtension.class);
  }

  @Test
  public void testLoadDeltaCatalogCachesInstance() {
    CaseInsensitiveStringMap options =
        new CaseInsensitiveStringMap(
            ImmutableMap.of(
                DeltaHelper.DELTA_CATALOG_IMPL_KEY, "org.apache.polaris.spark.NoopDeltaCatalog"));
    DeltaHelper helper = new DeltaHelper(options);
    PolarisSparkCatalog polarisSparkCatalog = mock(PolarisSparkCatalog.class);

    TableCatalog deltaCatalog1 = helper.loadDeltaCatalog(polarisSparkCatalog);
    TableCatalog deltaCatalog2 = helper.loadDeltaCatalog(polarisSparkCatalog);

    // Should return the same cached instance
    assertThat(deltaCatalog1).isSameAs(deltaCatalog2);
  }

  @Test
  public void testLoadDeltaCatalogWithNonExistentClass() {
    CaseInsensitiveStringMap options =
        new CaseInsensitiveStringMap(
            ImmutableMap.of(
                DeltaHelper.DELTA_CATALOG_IMPL_KEY, "com.example.NonExistentDeltaCatalog"));
    DeltaHelper helper = new DeltaHelper(options);
    PolarisSparkCatalog polarisSparkCatalog = mock(PolarisSparkCatalog.class);

    assertThatThrownBy(() -> helper.loadDeltaCatalog(polarisSparkCatalog))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Cannot initialize Delta Catalog")
        .hasMessageContaining("com.example.NonExistentDeltaCatalog");
  }

  @Test
  public void testLoadDeltaCatalogWithNonTableCatalogClass() {
    // Use a class that exists but doesn't implement TableCatalog
    CaseInsensitiveStringMap options =
        new CaseInsensitiveStringMap(
            ImmutableMap.of(DeltaHelper.DELTA_CATALOG_IMPL_KEY, "java.lang.String"));
    DeltaHelper helper = new DeltaHelper(options);
    PolarisSparkCatalog polarisSparkCatalog = mock(PolarisSparkCatalog.class);

    assertThatThrownBy(() -> helper.loadDeltaCatalog(polarisSparkCatalog))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Cannot initialize Delta Catalog")
        .hasMessageContaining("java.lang.String");
  }

  @Test
  public void testLoadDeltaCatalogSetsIsUnityCatalogField() throws Exception {
    CaseInsensitiveStringMap options =
        new CaseInsensitiveStringMap(
            ImmutableMap.of(
                DeltaHelper.DELTA_CATALOG_IMPL_KEY, "org.apache.polaris.spark.NoopDeltaCatalog"));
    DeltaHelper helper = new DeltaHelper(options);
    PolarisSparkCatalog polarisSparkCatalog = mock(PolarisSparkCatalog.class);

    TableCatalog deltaCatalog = helper.loadDeltaCatalog(polarisSparkCatalog);

    // Verify that the isUnityCatalog field is set to true using reflection
    java.lang.reflect.Field field = deltaCatalog.getClass().getDeclaredField("isUnityCatalog");
    field.setAccessible(true);
    boolean isUnityCatalog = (boolean) field.get(deltaCatalog);

    assertThat(isUnityCatalog).isTrue();
  }

  @Test
  public void testLoadDeltaCatalogWithCaseInsensitiveOptions() {
    // Test that options are case-insensitive
    CaseInsensitiveStringMap options =
        new CaseInsensitiveStringMap(
            ImmutableMap.of("DELTA-CATALOG-IMPL", "org.apache.polaris.spark.NoopDeltaCatalog"));
    DeltaHelper helper = new DeltaHelper(options);
    PolarisSparkCatalog polarisSparkCatalog = mock(PolarisSparkCatalog.class);

    TableCatalog deltaCatalog = helper.loadDeltaCatalog(polarisSparkCatalog);

    assertThat(deltaCatalog).isNotNull();
    assertThat(deltaCatalog).isInstanceOf(NoopDeltaCatalog.class);
  }

  @Test
  public void testMultipleDeltaHelperInstances() {
    CaseInsensitiveStringMap options1 =
        new CaseInsensitiveStringMap(
            ImmutableMap.of(
                DeltaHelper.DELTA_CATALOG_IMPL_KEY, "org.apache.polaris.spark.NoopDeltaCatalog"));
    CaseInsensitiveStringMap options2 =
        new CaseInsensitiveStringMap(
            ImmutableMap.of(
                DeltaHelper.DELTA_CATALOG_IMPL_KEY, "org.apache.polaris.spark.NoopDeltaCatalog"));

    DeltaHelper helper1 = new DeltaHelper(options1);
    DeltaHelper helper2 = new DeltaHelper(options2);

    PolarisSparkCatalog polarisSparkCatalog = mock(PolarisSparkCatalog.class);

    TableCatalog deltaCatalog1 = helper1.loadDeltaCatalog(polarisSparkCatalog);
    TableCatalog deltaCatalog2 = helper2.loadDeltaCatalog(polarisSparkCatalog);

    // Different helper instances should create different delta catalog instances
    assertThat(deltaCatalog1).isNotSameAs(deltaCatalog2);
  }
}
