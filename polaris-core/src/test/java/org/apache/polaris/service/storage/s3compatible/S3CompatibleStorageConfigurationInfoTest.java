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
package org.apache.polaris.service.storage.s3compatible;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.apache.polaris.core.storage.s3compatible.S3CompatibleStorageConfigurationInfo;
import org.junit.jupiter.api.Test;

public class S3CompatibleStorageConfigurationInfoTest {

  @Test
  public void testS3CompatibleStorageConfigurationInfo() {
    String warehouseDir = "s3://bucket/path/to/warehouse";
    S3CompatibleStorageConfigurationInfo conf =
        new S3CompatibleStorageConfigurationInfo(
            "http://localhost:9000",
            null,
            "MINIO_S3_CATALOG_1_ID",
            "MINIO_S3_CATALOG_1_SECRET",
            true,
            null,
            null,
            List.of(warehouseDir));
    assertThat(conf).isNotNull();
    assertThat(conf.getS3Endpoint()).isEqualTo("http://localhost:9000");
    assertThat(conf.getS3ProfileName()).isNull();
    assertThat(conf.getS3CredentialsCatalogAccessKeyId()).isEqualTo("MINIO_S3_CATALOG_1_ID");
    assertThat(conf.getS3CredentialsCatalogSecretAccessKey())
        .isEqualTo("MINIO_S3_CATALOG_1_SECRET");
    assertThat(conf.getS3PathStyleAccess()).isTrue();
    assertThat(conf.getS3Region()).isNull();
    assertThat(conf.getS3RoleArn()).isEqualTo("");
  }
}
