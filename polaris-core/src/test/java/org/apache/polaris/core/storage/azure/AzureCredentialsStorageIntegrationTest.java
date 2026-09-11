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

import static org.apache.polaris.core.config.FeatureConfiguration.AZURE_RETRY_COUNT;
import static org.apache.polaris.core.config.FeatureConfiguration.AZURE_RETRY_DELAY_MILLIS;
import static org.apache.polaris.core.config.FeatureConfiguration.AZURE_RETRY_JITTER_FACTOR;
import static org.apache.polaris.core.config.FeatureConfiguration.AZURE_TIMEOUT_MILLIS;
import static org.apache.polaris.core.config.FeatureConfiguration.STORAGE_CREDENTIAL_DURATION_SECONDS;
import static org.apache.polaris.core.storage.azure.AzureCredentialsStorageIntegration.toAccessConfig;

import com.azure.core.credential.AccessToken;
import com.azure.core.credential.TokenRequestContext;
import com.azure.identity.DefaultAzureCredential;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobContainerClientBuilder;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.models.UserDelegationKey;
import com.azure.storage.blob.sas.BlobServiceSasSignatureValues;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Optional;
import java.util.Set;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.storage.StorageAccessConfig;
import org.apache.polaris.core.storage.StorageAccessProperty;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.Answers;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;
import reactor.core.publisher.Mono;

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
    Assertions.assertThat(adlsNoRefreshResult.credentials()).hasSize(5);
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
    Assertions.assertThat(adlsWithRefreshResult.credentials()).hasSize(5);
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
    Assertions.assertThat(blobResult.credentials()).hasSize(5);
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

  @ParameterizedTest
  @CsvSource({"3600,3900", "604800,604740"})
  void computeUsesAdjustedKeyStartAndMatchingExpiry(
      int configuredDurationSeconds, int expectedKeyDurationSeconds) {
    VendedCredentials vendedCredentials = vendCredentials(configuredDurationSeconds);

    Assertions.assertThat(
            vendedCredentials
                .keyStart()
                .toInstant()
                .until(vendedCredentials.sasExpiry().toInstant(), ChronoUnit.SECONDS))
        .isEqualTo(expectedKeyDurationSeconds);
    Assertions.assertThat(vendedCredentials.keyEnd()).isEqualTo(vendedCredentials.sasExpiry());
    Assertions.assertThat(vendedCredentials.accessConfig().expiresAt())
        .contains(vendedCredentials.sasExpiry().toInstant());
  }

  private static VendedCredentials vendCredentials(int configuredDurationSeconds) {
    DefaultAzureCredential credential = Mockito.mock(DefaultAzureCredential.class);
    Mockito.when(credential.getToken(Mockito.any(TokenRequestContext.class)))
        .thenReturn(
            Mono.just(
                new AccessToken("access-token", OffsetDateTime.now().plus(Duration.ofHours(1)))));

    BlobServiceClient blobServiceClient = Mockito.mock(BlobServiceClient.class);
    UserDelegationKey userDelegationKey = Mockito.mock(UserDelegationKey.class);
    Mockito.when(blobServiceClient.getUserDelegationKey(Mockito.any(), Mockito.any()))
        .thenReturn(userDelegationKey);

    BlobContainerClient blobContainerClient = Mockito.mock(BlobContainerClient.class);
    Mockito.when(
            blobContainerClient.generateUserDelegationSas(
                Mockito.any(BlobServiceSasSignatureValues.class), Mockito.same(userDelegationKey)))
        .thenReturn("sas-token");

    RealmConfig realmConfig = realmConfigWithDuration(configuredDurationSeconds);
    AzureStorageConfigurationInfo storageConfig =
        AzureStorageConfigurationInfo.builder()
            .addAllowedLocation("wasbs://container@account.blob.core.windows.net/path")
            .tenantId("tenant-id")
            .build();
    AzureStorageCredentialCacheKey key =
        AzureStorageCredentialCacheKey.of(
            "realm",
            storageConfig,
            false,
            Set.of("wasbs://container@account.blob.core.windows.net/path"),
            Set.of(),
            Optional.empty(),
            credential,
            realmConfig);

    try (MockedConstruction<BlobServiceClientBuilder> ignored =
            Mockito.mockConstruction(
                BlobServiceClientBuilder.class,
                Mockito.withSettings().defaultAnswer(Answers.RETURNS_SELF),
                (builder, context) ->
                    Mockito.when(builder.buildClient()).thenReturn(blobServiceClient));
        MockedConstruction<BlobContainerClientBuilder> ignoredContainerBuilder =
            Mockito.mockConstruction(
                BlobContainerClientBuilder.class,
                Mockito.withSettings().defaultAnswer(Answers.RETURNS_SELF),
                (builder, context) ->
                    Mockito.when(builder.buildClient()).thenReturn(blobContainerClient))) {
      StorageAccessConfig accessConfig = AzureCredentialsStorageIntegration.compute(key);

      ArgumentCaptor<OffsetDateTime> keyStartCaptor = ArgumentCaptor.forClass(OffsetDateTime.class);
      ArgumentCaptor<OffsetDateTime> keyEndCaptor = ArgumentCaptor.forClass(OffsetDateTime.class);
      Mockito.verify(blobServiceClient)
          .getUserDelegationKey(keyStartCaptor.capture(), keyEndCaptor.capture());
      ArgumentCaptor<BlobServiceSasSignatureValues> sasValuesCaptor =
          ArgumentCaptor.forClass(BlobServiceSasSignatureValues.class);
      Mockito.verify(blobContainerClient)
          .generateUserDelegationSas(sasValuesCaptor.capture(), Mockito.same(userDelegationKey));
      return new VendedCredentials(
          keyStartCaptor.getValue(),
          keyEndCaptor.getValue(),
          sasValuesCaptor.getValue().getExpiryTime(),
          accessConfig);
    }
  }

  private static RealmConfig realmConfigWithDuration(int durationSeconds) {
    RealmConfig realmConfig = Mockito.mock(RealmConfig.class);
    Mockito.when(realmConfig.getConfig(AZURE_TIMEOUT_MILLIS)).thenReturn(1_000);
    Mockito.when(realmConfig.getConfig(AZURE_RETRY_COUNT)).thenReturn(0);
    Mockito.when(realmConfig.getConfig(AZURE_RETRY_DELAY_MILLIS)).thenReturn(1);
    Mockito.when(realmConfig.getConfig(AZURE_RETRY_JITTER_FACTOR)).thenReturn(0.0);
    Mockito.when(realmConfig.getConfig(STORAGE_CREDENTIAL_DURATION_SECONDS))
        .thenReturn(durationSeconds);
    return realmConfig;
  }

  private record VendedCredentials(
      OffsetDateTime keyStart,
      OffsetDateTime keyEnd,
      OffsetDateTime sasExpiry,
      StorageAccessConfig accessConfig) {}
}
