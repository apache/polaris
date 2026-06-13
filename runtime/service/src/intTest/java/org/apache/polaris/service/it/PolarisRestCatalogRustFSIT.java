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
package org.apache.polaris.service.it;

import static org.apache.polaris.test.commons.MinioRustProfile.ACCESS_KEY;
import static org.apache.polaris.test.commons.MinioRustProfile.SECRET_KEY;

import com.google.common.collect.ImmutableMap;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.common.ResourceArg;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.quarkus.test.junit.TestProfile;
import java.util.List;
import java.util.Map;
import org.apache.polaris.core.admin.model.AwsStorageConfigInfo;
import org.apache.polaris.core.admin.model.StorageConfigInfo;
import org.apache.polaris.core.storage.StorageAccessProperty;
import org.apache.polaris.service.it.ext.PolarisIntegrationTestExtension;
import org.apache.polaris.service.it.test.PolarisRestCatalogIntegrationBase;
import org.apache.polaris.test.commons.MinioRustProfile;
import org.apache.polaris.test.rustfs.Rustfs;
import org.apache.polaris.test.rustfs.RustfsAccess;
import org.apache.polaris.test.rustfs.RustfsConditionExtension;
import org.apache.polaris.test.rustfs.RustfsTestResource;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;

@QuarkusIntegrationTest
@TestProfile(MinioRustProfile.class)
@QuarkusTestResource(
    value = RustfsTestResource.class,
    initArgs = {
      @ResourceArg(name = "accessKey", value = ACCESS_KEY),
      @ResourceArg(name = "secretKey", value = SECRET_KEY)
    })
@ExtendWith(RustfsConditionExtension.class)
@ExtendWith(PolarisIntegrationTestExtension.class)
public class PolarisRestCatalogRustFSIT extends PolarisRestCatalogIntegrationBase {

  protected static final String BUCKET_URI_PREFIX = "/rustfs-test-polaris";

  @Rustfs static RustfsAccess rustfsAccess;

  @Override
  protected ImmutableMap.Builder<String, String> clientFileIOProperties() {
    return super.clientFileIOProperties()
        .put(StorageAccessProperty.AWS_ENDPOINT.getPropertyName(), rustfsAccess.s3endpoint())
        .put(StorageAccessProperty.AWS_PATH_STYLE_ACCESS.getPropertyName(), "true")
        .put(StorageAccessProperty.AWS_KEY_ID.getPropertyName(), ACCESS_KEY)
        .put(StorageAccessProperty.AWS_SECRET_KEY.getPropertyName(), SECRET_KEY);
  }

  @Override
  protected StorageConfigInfo getStorageConfigInfo() {
    AwsStorageConfigInfo.Builder storageConfig =
        AwsStorageConfigInfo.builder()
            .setStorageType(StorageConfigInfo.StorageTypeEnum.S3)
            .setPathStyleAccess(true)
            .setEndpoint(rustfsAccess.s3endpoint())
            .setAllowedLocations(List.of(rustfsAccess.s3BucketUri(BUCKET_URI_PREFIX).toString()));

    return storageConfig.build();
  }

  @Override
  protected Map<String, String> extraCatalogProperties(TestInfo testInfo) {
    return ImmutableMap.<String, String>builder()
        .put(StorageAccessProperty.AWS_ENDPOINT.getPropertyName(), rustfsAccess.s3endpoint())
        .put(StorageAccessProperty.AWS_PATH_STYLE_ACCESS.getPropertyName(), "true")
        .put(StorageAccessProperty.AWS_KEY_ID.getPropertyName(), ACCESS_KEY)
        .put(StorageAccessProperty.AWS_SECRET_KEY.getPropertyName(), SECRET_KEY)
        .build();
  }
}
