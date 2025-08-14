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

import com.google.common.collect.ImmutableMap;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import java.net.URI;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.io.FileIO;
import org.apache.polaris.core.admin.model.AwsStorageConfigInfo;
import org.apache.polaris.core.admin.model.StorageConfigInfo;
import org.apache.polaris.core.storage.StorageAccessProperty;
import org.apache.polaris.service.it.ext.PolarisIntegrationTestExtension;
import org.apache.polaris.service.it.test.PolarisRestCatalogIntegrationBase;
import org.apache.polaris.test.minio.Minio;
import org.apache.polaris.test.minio.MinioAccess;
import org.apache.polaris.test.minio.MinioExtension;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.ExtendWith;

@QuarkusIntegrationTest
@TestProfile(PolarisRestCatalogMinIOIT.Profile.class)
@ExtendWith(MinioExtension.class)
@ExtendWith(PolarisIntegrationTestExtension.class)
public class PolarisRestCatalogMinIOIT extends PolarisRestCatalogIntegrationBase {

  private static final String BUCKET_URI_PREFIX = "/minio-test-polaris";
  private static final String MINIO_ACCESS_KEY = "test-ak-123-polaris";
  private static final String MINIO_SECRET_KEY = "test-sk-123-polaris";

  public static class Profile implements QuarkusTestProfile {

    @Override
    public Map<String, String> getConfigOverrides() {
      return ImmutableMap.<String, String>builder()
          .put("polaris.storage.aws.access-key", MINIO_ACCESS_KEY)
          .put("polaris.storage.aws.secret-key", MINIO_SECRET_KEY)
          .put("polaris.features.\"SKIP_CREDENTIAL_SUBSCOPING_INDIRECTION\"", "false")
          .build();
    }
  }

  private static URI storageBase;
  private static String endpoint;

  @BeforeAll
  static void setup(
      @Minio(accessKey = MINIO_ACCESS_KEY, secretKey = MINIO_SECRET_KEY) MinioAccess minioAccess) {
    storageBase = minioAccess.s3BucketUri(BUCKET_URI_PREFIX);
    endpoint = minioAccess.s3endpoint();
  }

  @Override
  protected void initializeClientFileIO(FileIO fileIO) {
    fileIO.initialize(
        ImmutableMap.<String, String>builder()
            .put(StorageAccessProperty.AWS_ENDPOINT.getPropertyName(), endpoint)
            .put(StorageAccessProperty.AWS_PATH_STYLE_ACCESS.getPropertyName(), "true")
            .put(StorageAccessProperty.AWS_KEY_ID.getPropertyName(), MINIO_ACCESS_KEY)
            .put(StorageAccessProperty.AWS_SECRET_KEY.getPropertyName(), MINIO_SECRET_KEY)
            .build());
  }

  @Override
  protected StorageConfigInfo getStorageConfigInfo() {
    AwsStorageConfigInfo.Builder storageConfig =
        AwsStorageConfigInfo.builder()
            .setStorageType(StorageConfigInfo.StorageTypeEnum.S3)
            .setPathStyleAccess(true)
            .setEndpoint(endpoint)
            .setAllowedLocations(List.of(storageBase.toString()));

    return storageConfig.build();
  }
}
