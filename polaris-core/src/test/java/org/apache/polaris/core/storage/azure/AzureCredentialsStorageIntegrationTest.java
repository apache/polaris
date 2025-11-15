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

package org.apache.polaris.core.storage.azure;

import static org.apache.polaris.core.storage.azure.AzureCredentialsStorageIntegration.toAccessConfig;

import java.time.Instant;
import java.util.Optional;
import org.apache.polaris.core.storage.StorageAccessConfig;
import org.apache.polaris.core.storage.StorageAccessProperty;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class AzureCredentialsStorageIntegrationTest {

  @Test
  public void testAzureCredentialFormatting() {
    Instant expiresAt = Instant.ofEpochMilli(Long.MAX_VALUE);

    StorageAccessConfig noSuffixResult =
        toAccessConfig("sasToken", "some_account", expiresAt, Optional.empty());
    Assertions.assertThat(noSuffixResult.credentials()).hasSize(3);
    Assertions.assertThat(noSuffixResult.credentials()).containsKey("adls.sas-token.some_account");
    Assertions.assertThat(noSuffixResult.credentials())
        .containsKey("adls.sas-token-expires-at-ms.some_account");
    Assertions.assertThat(noSuffixResult.credentials())
        .doesNotContainKey(
            StorageAccessProperty.AZURE_REFRESH_CREDENTIALS_ENDPOINT.getPropertyName());

    StorageAccessConfig adlsSuffixResult =
        toAccessConfig(
            "sasToken",
            "some_account." + AzureLocation.ADLS_ENDPOINT,
            expiresAt,
            Optional.of("endpoint/credentials"));
    Assertions.assertThat(adlsSuffixResult.credentials()).hasSize(4);
    Assertions.assertThat(adlsSuffixResult.credentials())
        .containsKey("adls.sas-token.some_account");
    Assertions.assertThat(noSuffixResult.credentials())
        .containsKey("adls.sas-token-expires-at-ms.some_account");
    Assertions.assertThat(adlsSuffixResult.credentials())
        .containsKey("adls.sas-token.some_account." + AzureLocation.ADLS_ENDPOINT);

    Assertions.assertThat(adlsSuffixResult.extraProperties())
        .containsEntry(
            StorageAccessProperty.AZURE_REFRESH_CREDENTIALS_ENDPOINT.getPropertyName(),
            "endpoint/credentials");

    StorageAccessConfig blobSuffixResult =
        toAccessConfig(
            "sasToken", "some_account." + AzureLocation.BLOB_ENDPOINT, expiresAt, Optional.empty());
    Assertions.assertThat(blobSuffixResult.credentials()).hasSize(4);
    Assertions.assertThat(blobSuffixResult.credentials())
        .containsKey("adls.sas-token.some_account");
    Assertions.assertThat(blobSuffixResult.credentials())
        .containsKey("adls.sas-token.some_account." + AzureLocation.BLOB_ENDPOINT);
    Assertions.assertThat(blobSuffixResult.credentials())
        .containsKey("adls.sas-token-expires-at-ms.some_account.blob.core.windows.net");
  }
}
