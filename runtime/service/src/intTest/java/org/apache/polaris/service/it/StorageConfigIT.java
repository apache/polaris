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
import java.util.Map;
import org.apache.polaris.service.it.test.PolarisStorageConfigIntegrationTest;
import org.apache.polaris.test.minio.Minio;
import org.apache.polaris.test.minio.MinioAccess;
import org.apache.polaris.test.minio.MinioExtension;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.ExtendWith;

@QuarkusIntegrationTest
@TestProfile(StorageConfigIT.Profile.class)
@ExtendWith(MinioExtension.class)
public class StorageConfigIT extends PolarisStorageConfigIntegrationTest {
  private static final String MINIO_ACCESS_KEY = "storage-config-ak";
  private static final String MINIO_SECRET_KEY = "storage-config-sk";
  private static final String S3_BASE_LOCATION_PROPERTY = "polaris.it.storage.s3.base-location";
  private static final String S3_ENDPOINT_PROPERTY = "polaris.it.storage.s3.endpoint";
  private static final String S3_PATH_STYLE_ACCESS_PROPERTY =
      "polaris.it.storage.s3.path-style-access";
  private static final String S3_DATA_PLANE_ENABLED_PROPERTY = "polaris.it.storage.s3.enabled";

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

  @BeforeAll
  static void setupMinio(
      @Minio(accessKey = MINIO_ACCESS_KEY, secretKey = MINIO_SECRET_KEY) MinioAccess minioAccess) {
    URI storageBase = minioAccess.s3BucketUri("/storage-config-it");
    System.setProperty(S3_BASE_LOCATION_PROPERTY, storageBase.toString());
    System.setProperty(S3_ENDPOINT_PROPERTY, minioAccess.s3endpoint());
    System.setProperty(S3_PATH_STYLE_ACCESS_PROPERTY, "true");
    System.setProperty(S3_DATA_PLANE_ENABLED_PROPERTY, "true");
  }
}
