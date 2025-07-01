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

import java.net.URI;
import java.nio.file.Path;
import java.util.List;
import org.apache.polaris.core.admin.model.FileStorageConfigInfo;
import org.apache.polaris.core.admin.model.StorageConfigInfo;
import org.apache.polaris.service.it.env.IntegrationTestsHelper;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;

/** Runs PolarisRestCatalogIntegrationBase test on the local filesystem. */
public class PolarisRestCatalogFileIntegrationTest extends PolarisRestCatalogIntegrationBase {

  public static String baseLocation;

  @BeforeAll
  static void setup(@TempDir Path tempDir) {
    URI tempDirURI = IntegrationTestsHelper.getTemporaryDirectory(tempDir);
    baseLocation = stripTrailingSlash(tempDirURI.toString());
  }

  @Override
  protected StorageConfigInfo getStorageConfigInfo() {
    return FileStorageConfigInfo.builder()
        .setStorageType(StorageConfigInfo.StorageTypeEnum.FILE)
        .setAllowedLocations(List.of(baseLocation, "file://"))
        .build();
  }

  @Override
  protected boolean shouldSkip() {
    return false;
  }

  private static String stripTrailingSlash(String uri) {
    return uri.endsWith("/") && uri.length() > "file:///".length()
        ? uri.substring(0, uri.length() - 1)
        : uri;
  }
}
