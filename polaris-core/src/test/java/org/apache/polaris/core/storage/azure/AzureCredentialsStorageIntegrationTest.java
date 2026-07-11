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
    AzureLocation adlsLocation =
        new AzureLocation("abfss://container@myaccount." + AzureLocation.ADLS_ENDPOINT + "/path");
    AzureLocation blobLocation =
        new AzureLocation("wasbs://container@myaccount." + AzureLocation.BLOB_ENDPOINT + "/path");

    // ADLS location without refresh credentials endpoint.
    StorageAccessConfig adlsNoRefreshResult =
        toAccessConfig("sasToken", adlsLocation, expiresAt, Optional.empty());
    Assertions.assertThat(adlsNoRefreshResult.credentials()).hasSize(6);
    Assertions.assertThat(adlsNoRefreshResult.credentials())
        .containsKey("adls.sas-token.myaccount." + AzureLocation.ADLS_ENDPOINT);
    Assertions.assertThat(adlsNoRefreshResult.credentials())
        .containsKey("adls.sas-token-expires-at-ms.myaccount." + AzureLocation.ADLS_ENDPOINT);
    Assertions.assertThat(adlsNoRefreshResult.credentials())
        .containsKey("adls.sas-token.myaccount");
    Assertions.assertThat(adlsNoRefreshResult.credentials())
        .containsKey(StorageAccessProperty.AZURE_SAS_TOKEN_BARE.getPropertyName());
    Assertions.assertThat(adlsNoRefreshResult.credentials())
        .containsEntry(StorageAccessProperty.AZURE_SAS_TOKEN_BARE.getPropertyName(), "sasToken");
    Assertions.assertThat(adlsNoRefreshResult.credentials())
        .containsEntry(StorageAccessProperty.AZURE_ACCOUNT_NAME.getPropertyName(), "myaccount");
    Assertions.assertThat(adlsNoRefreshResult.credentials())
        .doesNotContainKey(
            StorageAccessProperty.AZURE_REFRESH_CREDENTIALS_ENDPOINT.getPropertyName());

    // ADLS location with refresh credentials endpoint.
    StorageAccessConfig adlsWithRefreshResult =
        toAccessConfig("sasToken", adlsLocation, expiresAt, Optional.of("endpoint/credentials"));
    Assertions.assertThat(adlsWithRefreshResult.credentials()).hasSize(6);
    Assertions.assertThat(adlsWithRefreshResult.credentials())
        .containsKey("adls.sas-token.myaccount");
    Assertions.assertThat(adlsWithRefreshResult.credentials())
        .containsKey("adls.sas-token-expires-at-ms.myaccount." + AzureLocation.ADLS_ENDPOINT);
    Assertions.assertThat(adlsWithRefreshResult.credentials())
        .containsKey("adls.sas-token.myaccount." + AzureLocation.ADLS_ENDPOINT);
    Assertions.assertThat(adlsWithRefreshResult.credentials())
        .containsEntry(StorageAccessProperty.AZURE_SAS_TOKEN_BARE.getPropertyName(), "sasToken");
    Assertions.assertThat(adlsWithRefreshResult.credentials())
        .containsEntry(StorageAccessProperty.AZURE_ACCOUNT_NAME.getPropertyName(), "myaccount");

    Assertions.assertThat(adlsWithRefreshResult.extraProperties())
        .containsEntry(
            StorageAccessProperty.AZURE_REFRESH_CREDENTIALS_ENDPOINT.getPropertyName(),
            "endpoint/credentials");

    // Blob location.
    StorageAccessConfig blobResult =
        toAccessConfig("sasToken", blobLocation, expiresAt, Optional.empty());
    Assertions.assertThat(blobResult.credentials()).hasSize(6);
    Assertions.assertThat(blobResult.credentials()).containsKey("adls.sas-token.myaccount");
    Assertions.assertThat(blobResult.credentials())
        .containsKey("adls.sas-token.myaccount." + AzureLocation.BLOB_ENDPOINT);
    Assertions.assertThat(blobResult.credentials())
        .containsKey("adls.sas-token-expires-at-ms.myaccount.blob.core.windows.net");
    Assertions.assertThat(blobResult.credentials())
        .containsEntry(StorageAccessProperty.AZURE_SAS_TOKEN_BARE.getPropertyName(), "sasToken");
    Assertions.assertThat(blobResult.credentials())
        .containsEntry(StorageAccessProperty.AZURE_ACCOUNT_NAME.getPropertyName(), "myaccount");
  }
}
