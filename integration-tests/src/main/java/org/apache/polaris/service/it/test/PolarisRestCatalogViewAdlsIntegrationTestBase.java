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

import java.io.File;
import java.util.List;
import org.apache.polaris.core.admin.model.AzureStorageConfigInfo;
import org.apache.polaris.core.admin.model.StorageConfigInfo;
import org.apache.polaris.core.storage.StorageUtil;

/** Runs PolarisRestCatalogViewIntegrationTest on Azure. */
public abstract class PolarisRestCatalogViewAdlsIntegrationTestBase
    extends PolarisRestCatalogViewIntegrationBase {
  public static final String TENANT_ID = System.getenv("INTEGRATION_TEST_ADLS_TENANT_ID");
  public static final String BASE_LOCATION = System.getenv("INTEGRATION_TEST_ADLS_PATH");

  @Override
  protected StorageConfigInfo getStorageConfigInfo() {
    return AzureStorageConfigInfo.builder()
        .setTenantId(TENANT_ID)
        .setStorageType(StorageConfigInfo.StorageTypeEnum.AZURE)
        .setAllowedLocations(
            List.of(
                StorageUtil.concatFilePrefixes(BASE_LOCATION, POLARIS_IT_SUBDIR, File.separator)))
        .build();
  }

  @Override
  protected String getCustomMetadataLocationDir() {
    return StorageUtil.concatFilePrefixes(BASE_LOCATION, POLARIS_IT_SUBDIR, File.separator);
  }
}
