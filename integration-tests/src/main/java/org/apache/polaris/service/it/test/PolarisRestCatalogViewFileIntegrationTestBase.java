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
package org.apache.polaris.service.it.test;

import java.nio.file.Path;
import java.util.List;
import org.apache.polaris.core.admin.model.FileStorageConfigInfo;
import org.apache.polaris.core.admin.model.StorageConfigInfo;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;

/** Runs PolarisRestCatalogViewIntegrationTest on the local filesystem. */
public abstract class PolarisRestCatalogViewFileIntegrationTestBase
    extends PolarisRestCatalogViewIntegrationBase {
  static String baseLocation;

  @BeforeAll
  public static void setUp(@TempDir Path tempDir) {
    String baseUri = tempDir.toAbsolutePath().toUri().toString();
    if (baseUri.endsWith("/")) {
      baseUri = baseUri.substring(0, baseUri.length() - 1);
    }
    baseLocation = baseUri;
  }

  @Override
  protected StorageConfigInfo getStorageConfigInfo() {
    return FileStorageConfigInfo.builder()
        .setStorageType(StorageConfigInfo.StorageTypeEnum.FILE)
        .setAllowedLocations(List.of(baseLocation))
        .build();
  }

  @Override
  protected boolean shouldSkip() {
    return false;
  }
}
