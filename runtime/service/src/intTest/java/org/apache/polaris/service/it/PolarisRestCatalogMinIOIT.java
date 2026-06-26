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
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.apache.polaris.core.admin.model.AwsStorageConfigInfo;
import org.apache.polaris.core.admin.model.StorageConfigInfo;
import org.apache.polaris.service.it.env.RestCatalogConfig;
import org.apache.polaris.service.it.ext.PolarisIntegrationTestExtension;
import org.apache.polaris.service.it.test.PolarisRestCatalogIntegrationBase;
import org.apache.polaris.test.commons.MinioRustProfile;
import org.apache.polaris.test.minio.Minio;
import org.apache.polaris.test.minio.MinioAccess;
import org.apache.polaris.test.minio.MinioConditionExtension;
import org.apache.polaris.test.minio.MinioTestResource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;

@QuarkusIntegrationTest
@TestProfile(MinioRustProfile.class)
@QuarkusTestResource(
    value = MinioTestResource.class,
    initArgs = {
      @ResourceArg(name = "accessKey", value = ACCESS_KEY),
      @ResourceArg(name = "secretKey", value = SECRET_KEY)
    })
@ExtendWith(MinioConditionExtension.class)
@ExtendWith(PolarisIntegrationTestExtension.class)
@RestCatalogConfig({"header.X-Iceberg-Access-Delegation", "vended-credentials"})
public class PolarisRestCatalogMinIOIT extends PolarisRestCatalogIntegrationBase {

  protected static final String BUCKET_URI_PREFIX = "/minio-test-polaris";

  @Minio static MinioAccess minioAccess;

  private static Map<String, String> s3Properties;

  @BeforeEach
  void setup() {
    s3Properties =
        Map.of(
            S3FileIOProperties.ENDPOINT,
            minioAccess.s3endpoint(),
            S3FileIOProperties.PATH_STYLE_ACCESS,
            "true",
            S3FileIOProperties.ACCESS_KEY_ID,
            ACCESS_KEY,
            S3FileIOProperties.SECRET_ACCESS_KEY,
            SECRET_KEY);
  }

  @Override
  protected ImmutableMap.Builder<String, String> clientFileIOProperties() {
    return super.clientFileIOProperties().putAll(s3Properties);
  }

  @Override
  protected StorageConfigInfo getStorageConfigInfo() {
    AwsStorageConfigInfo.Builder storageConfig =
        AwsStorageConfigInfo.builder()
            .setStorageType(StorageConfigInfo.StorageTypeEnum.S3)
            .setPathStyleAccess(true)
            .setEndpoint(minioAccess.s3endpoint())
            .setAllowedLocations(List.of(minioAccess.s3BucketUri(BUCKET_URI_PREFIX).toString()));

    return storageConfig.build();
  }
}
