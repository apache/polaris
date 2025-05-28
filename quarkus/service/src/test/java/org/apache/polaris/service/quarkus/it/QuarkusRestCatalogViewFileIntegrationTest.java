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
package org.apache.polaris.service.quarkus.it;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import java.lang.reflect.Field;
import java.nio.file.Path;
import java.util.Map;
import org.apache.iceberg.view.ViewCatalogTests;
import org.apache.polaris.service.it.test.PolarisRestCatalogViewFileIntegrationTestBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;

@QuarkusTest
@TestProfile(QuarkusRestCatalogViewFileIntegrationTest.Profile.class)
public class QuarkusRestCatalogViewFileIntegrationTest
    extends PolarisRestCatalogViewFileIntegrationTestBase {

  public static class Profile implements QuarkusTestProfile {

    @Override
    public Map<String, String> getConfigOverrides() {
      return Map.of(
          "polaris.features.\"ALLOW_INSECURE_STORAGE_TYPES\"",
          "true",
          "polaris.features.\"SUPPORTED_CATALOG_STORAGE_TYPES\"",
          "[\"FILE\"]",
          "polaris.readiness.ignore-severe-issues",
          "true");
    }
  }

  @BeforeEach
  public void setUpTempDir(@TempDir Path tempDir) throws Exception {
    // see https://github.com/quarkusio/quarkus/issues/13261
    Field field = ViewCatalogTests.class.getDeclaredField("tempDir");
    field.setAccessible(true);
    field.set(this, tempDir);
  }
}
