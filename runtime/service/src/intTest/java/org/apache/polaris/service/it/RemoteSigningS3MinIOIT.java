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
import java.util.Optional;
import org.apache.polaris.core.storage.StorageAccessProperty;
import org.apache.polaris.service.it.ext.PolarisIntegrationTestExtension;
import org.apache.polaris.service.it.test.PolarisRemoteSigningS3IntegrationTestBase;
import org.apache.polaris.test.minio.Minio;
import org.apache.polaris.test.minio.MinioAccess;
import org.apache.polaris.test.minio.MinioExtension;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.ExtendWith;

@QuarkusIntegrationTest
@TestProfile(RemoteSigningS3MinIOIT.Profile.class)
@ExtendWith(MinioExtension.class)
@ExtendWith(PolarisIntegrationTestExtension.class)
public class RemoteSigningS3MinIOIT extends PolarisRemoteSigningS3IntegrationTestBase {

  private static final String BUCKET_URI_PREFIX = "/minio-test/";
  private static final String MINIO_ACCESS_KEY = "test-ak-123";
  private static final String MINIO_SECRET_KEY = "test-sk-123";

  public static class Profile implements QuarkusTestProfile {

    @Override
    public Map<String, String> getConfigOverrides() {
      return ImmutableMap.<String, String>builder()
          .putAll(DEFAULT_REMOTE_SIGNING_CONFIG)
          .put("polaris.storage.aws.access-key", MINIO_ACCESS_KEY)
          .put("polaris.storage.aws.secret-key", MINIO_SECRET_KEY)
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
  protected String storageBase() {
    return storageBase.toString();
  }

  @Override
  protected Optional<String> endpoint() {
    return Optional.of(endpoint);
  }

  @Override
  protected ImmutableMap.Builder<String, String> clientFileIOProperties() {
    return super.clientFileIOProperties()
        // Grant direct access to the MinIO bucket; this FileIO instance does not
        // use access delegation.
        .put(StorageAccessProperty.AWS_KEY_ID.getPropertyName(), MINIO_ACCESS_KEY)
        .put(StorageAccessProperty.AWS_SECRET_KEY.getPropertyName(), MINIO_SECRET_KEY);
  }
}
