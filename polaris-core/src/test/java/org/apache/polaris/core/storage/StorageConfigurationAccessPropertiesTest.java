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
package org.apache.polaris.core.storage;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import org.apache.polaris.core.storage.aws.AwsStorageConfigurationInfo;
import org.junit.jupiter.api.Test;

class StorageConfigurationAccessPropertiesTest {

  @Test
  void typedAwsFieldsWinOverBag() {
    AwsStorageConfigurationInfo config =
        AwsStorageConfigurationInfo.builder()
            .allowedLocations(List.of("s3://bucket/prefix"))
            .endpoint("https://typed.example")
            .pathStyleAccess(true)
            .region("us-west-2")
            .fileIoProperties(
                Map.of(
                    StorageAccessProperty.AWS_ENDPOINT.getPropertyName(),
                    "https://bag.example",
                    StorageAccessProperty.AWS_KEY_ID.getPropertyName(),
                    "bag-key",
                    StorageAccessProperty.AWS_SECRET_KEY.getPropertyName(),
                    "bag-secret",
                    "s3.something-custom",
                    "custom-value"))
            .build();

    StorageAccessConfig accessConfig =
        StorageConfigurationAccessProperties.storageConfigOnly(config);

    assertThat(accessConfig.extraProperties())
        .containsEntry(
            StorageAccessProperty.AWS_ENDPOINT.getPropertyName(), "https://typed.example")
        .containsEntry(StorageAccessProperty.AWS_PATH_STYLE_ACCESS.getPropertyName(), "true")
        .containsEntry(StorageAccessProperty.CLIENT_REGION.getPropertyName(), "us-west-2")
        .containsEntry("s3.something-custom", "custom-value");
    // Static keys stay server-only; they must not look like vended credentials.
    assertThat(accessConfig.credentials()).isEmpty();
    assertThat(accessConfig.internalProperties())
        .containsEntry(StorageAccessProperty.AWS_KEY_ID.getPropertyName(), "bag-key")
        .containsEntry(StorageAccessProperty.AWS_SECRET_KEY.getPropertyName(), "bag-secret");
    assertThat(accessConfig.supportsCredentialVending()).isFalse();
  }

  @Test
  void mergeTableDefaultsDoesNotOverrideAccessConfig() {
    StorageAccessConfig accessConfig =
        StorageAccessConfig.builder()
            .putExtraProperty(
                StorageAccessProperty.AWS_ENDPOINT.getPropertyName(), "https://access")
            .putCredential(StorageAccessProperty.AWS_KEY_ID.getPropertyName(), "access-key")
            .supportsCredentialVending(true)
            .build();

    StorageAccessConfig merged =
        StorageConfigurationAccessProperties.mergeTableDefaults(
            accessConfig,
            Map.of(
                StorageAccessProperty.AWS_ENDPOINT.getPropertyName(),
                "https://table-default",
                StorageAccessProperty.AWS_KEY_ID.getPropertyName(),
                "default-key",
                StorageAccessProperty.AWS_PATH_STYLE_ACCESS.getPropertyName(),
                "true"));

    assertThat(merged.extraProperties())
        .containsEntry(StorageAccessProperty.AWS_ENDPOINT.getPropertyName(), "https://access")
        .containsEntry(StorageAccessProperty.AWS_PATH_STYLE_ACCESS.getPropertyName(), "true");
    assertThat(merged.credentials())
        .containsEntry(StorageAccessProperty.AWS_KEY_ID.getPropertyName(), "access-key");
    assertThat(merged.internalProperties())
        .containsEntry(StorageAccessProperty.AWS_KEY_ID.getPropertyName(), "default-key");
  }
}
