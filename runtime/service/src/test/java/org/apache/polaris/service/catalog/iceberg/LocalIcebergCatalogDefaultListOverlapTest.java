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

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import java.util.HashMap;
import java.util.Map;
import org.apache.polaris.service.Profiles;

/**
 * Overlap test that runs with the optimized sibling check disabled, so location-overlap enforcement
 * goes through the default list-based path in {@code
 * LocalIcebergCatalog.validateNoLocationOverlap}. Inherits the assertions from {@link
 * AbstractLocalIcebergCatalogOverlapTest}. The dedicated optimized-path profiles ({@link
 * LocalIcebergCatalogOverlapTest}, {@link LocalIcebergCatalogRelationalOverlapTest}, {@link
 * LocalIcebergCatalogNoSqlOverlapTest}) all force {@code OPTIMIZED_SIBLING_CHECK=true}, so without
 * this profile the list-based path would run with enforcement enabled only in production, never in
 * tests.
 */
@QuarkusTest
@TestProfile(LocalIcebergCatalogDefaultListOverlapTest.DefaultListOverlapProfile.class)
public class LocalIcebergCatalogDefaultListOverlapTest
    extends AbstractLocalIcebergCatalogOverlapTest {

  /**
   * Profile that runs the default in-memory transactional backend with location-overlap enforcement
   * enabled and the optimized sibling check disabled.
   */
  public static class DefaultListOverlapProfile extends Profiles.DefaultIcebergCatalogProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      Map<String, String> overrides = new HashMap<>(super.getConfigOverrides());
      overrides.put("polaris.features.\"ALLOW_TABLE_LOCATION_OVERLAP\"", "false");
      overrides.put("polaris.features.\"OPTIMIZED_SIBLING_CHECK\"", "false");
      return overrides;
    }
  }
}
