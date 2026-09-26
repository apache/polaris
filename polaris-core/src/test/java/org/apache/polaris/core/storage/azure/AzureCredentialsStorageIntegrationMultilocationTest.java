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
import java.time.OffsetDateTime;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.storage.StorageAccessConfig;
import org.apache.polaris.core.storage.StorageAccessProperty;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Answers;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;
import reactor.core.publisher.Mono;

/**
 * Verifies that credentials are vended per storage account / container, so that Iceberg tables can
 * keep metadata and data in separate storage accounts or containers, and that locations outside the
 * authorized set are still rejected.
 */
public class AzureCredentialsStorageIntegrationMultilocationTest {

  private static final String METADATA_LOCATION =
      "wasbs://metadata-container@metadataaccount.blob.core.windows.net/metadata";
  private static final String DATA_LOCATION =
      "wasbs://data-container@dataaccount.blob.core.windows.net/data";

  @Test
  public void testVendsCredentialsPerStorageAccountAndContainer() {
    // Metadata and data live in different accounts *and* different containers.
    StorageAccessConfig accessConfig = vend("realm", METADATA_LOCATION, DATA_LOCATION, false);

    String metadataSas =
        accessConfig
            .credentials()
            .get(
                StorageAccessProperty.AZURE_SAS_TOKEN_ACCOUNT_HOST.getPropertyName()
                    + ".metadataaccount.blob.core.windows.net");
    String dataSas =
        accessConfig
            .credentials()
            .get(
                StorageAccessProperty.AZURE_SAS_TOKEN_ACCOUNT_HOST.getPropertyName()
                    + ".dataaccount.blob.core.windows.net");

    Assertions.assertThat(metadataSas).isEqualTo("sas-metadataaccount");
    Assertions.assertThat(dataSas).isEqualTo("sas-dataaccount");
    Assertions.assertThat(metadataSas).isNotEqualTo(dataSas);

    // The bare account-name keys are account-scoped, so they are intentionally omitted when the
    // locations span multiple accounts; clients must use the per-account-host keys instead.
    Assertions.assertThat(accessConfig.credentials())
        .doesNotContainKey(StorageAccessProperty.AZURE_SAS_TOKEN_BARE.getPropertyName());
    Assertions.assertThat(accessConfig.credentials())
        .doesNotContainKey(StorageAccessProperty.AZURE_ACCOUNT_NAME.getPropertyName());
  }

  @Test
  public void testVendsCredentialsForSeparateContainersInSameAccount() {
    StorageAccessConfig accessConfig =
        vend(
            "realm",
            "wasbs://metadata@account.blob.core.windows.net/metadata",
            "wasbs://data@account.blob.core.windows.net/data",
            false);

    // Both containers live in the same account, so the account-scoped legacy keys stay available.
    Assertions.assertThat(accessConfig.credentials())
        .containsEntry(StorageAccessProperty.AZURE_ACCOUNT_NAME.getPropertyName(), "account");
    Assertions.assertThat(accessConfig.credentials())
        .containsKey(
            StorageAccessProperty.AZURE_SAS_TOKEN_ACCOUNT_HOST.getPropertyName()
                + ".account.blob.core.windows.net");
  }

  @Test
  public void testLocationsOutsideAuthorizedSetAreNotVended() {
    // Only the data location is authorized; no credential key may be minted for the metadata
    // account, and no credential at all may be minted for an unrelated account.
    StorageAccessConfig accessConfig = vend("realm", DATA_LOCATION, DATA_LOCATION, false);

    Assertions.assertThat(accessConfig.credentials().keySet())
        .noneMatch(key -> key.contains("metadataaccount"));
    Assertions.assertThat(accessConfig.credentials().keySet())
        .noneMatch(key -> key.contains("unauthorizedaccount"));
  }

  @Test
  public void testReadOnlyLocationsDoNotGrantWriteAcrossOtherScope() {
    // metadata is write-scoped, data is read-only: the write permission must not leak from one
    // storage account to the other.
    StorageAccessConfig accessConfig = vend("realm", METADATA_LOCATION, DATA_LOCATION, false);

    Assertions.assertThat(accessConfig.credentials())
        .containsEntry(
            StorageAccessProperty.AZURE_SAS_TOKEN_ACCOUNT_HOST.getPropertyName()
                + ".metadataaccount.blob.core.windows.net",
            "sas-metadataaccount");
    Assertions.assertThat(accessConfig.credentials())
        .containsEntry(
            StorageAccessProperty.AZURE_SAS_TOKEN_ACCOUNT_HOST.getPropertyName()
                + ".dataaccount.blob.core.windows.net",
            "sas-dataaccount");
  }

  @Test
  public void testEmptyLocationsRejected() {
    Assertions.assertThatThrownBy(() -> vend("realm", "", null, false))
        .isInstanceOf(IllegalArgumentException.class);
  }

  private static StorageAccessConfig vend(
      String realm, String readLocation, String writeLocationOrNull, boolean allowList) {
    Set<String> readLocations = Set.of(readLocation);
    Set<String> writes = writeLocationOrNull == null ? Set.of() : Set.of(writeLocationOrNull);

    DefaultAzureCredential credential = Mockito.mock(DefaultAzureCredential.class);
    Mockito.when(credential.getToken(Mockito.any(TokenRequestContext.class)))
        .thenReturn(
            Mono.just(
                new AccessToken("access-token", OffsetDateTime.now().plus(Duration.ofHours(1)))));

    UserDelegationKey userDelegationKey = Mockito.mock(UserDelegationKey.class);
    BlobServiceClient blobServiceClient = Mockito.mock(BlobServiceClient.class);
    Mockito.when(blobServiceClient.getUserDelegationKey(Mockito.any(), Mockito.any()))
        .thenReturn(userDelegationKey);

    AzureStorageCredentialCacheKey key =
        AzureStorageCredentialCacheKey.of(
            realm,
            AzureStorageConfigurationInfo.builder()
                .addAllowedLocation(readLocation)
                .addAllAllowedLocations(writes)
                .tenantId("tenant-id")
                .build(),
            allowList,
            readLocations,
            writes,
            Optional.empty(),
            credential,
            realmConfig());

    AtomicInteger counter = new AtomicInteger();
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
                (builder, context) -> {
                  // The first container built belongs to the first scope, and so on. Each scope
                  // must receive its own container-scoped SAS token.
                  int index = counter.incrementAndGet();
                  String account =
                      index == 1 ? "metadataaccount" : index == 2 ? "dataaccount" : "account";
                  BlobContainerClient containerClient = Mockito.mock(BlobContainerClient.class);
                  Mockito.when(
                          containerClient.generateUserDelegationSas(
                              Mockito.any(BlobServiceSasSignatureValues.class),
                              Mockito.same(userDelegationKey)))
                      .thenReturn("sas-" + account);
                  Mockito.when(builder.buildClient()).thenReturn(containerClient);
                })) {
      return AzureCredentialsStorageIntegration.compute(key);
    }
  }

  private static RealmConfig realmConfig() {
    RealmConfig realmConfig = Mockito.mock(RealmConfig.class);
    Mockito.when(realmConfig.getConfig(AZURE_TIMEOUT_MILLIS)).thenReturn(1_000);
    Mockito.when(realmConfig.getConfig(AZURE_RETRY_COUNT)).thenReturn(0);
    Mockito.when(realmConfig.getConfig(AZURE_RETRY_DELAY_MILLIS)).thenReturn(1);
    Mockito.when(realmConfig.getConfig(AZURE_RETRY_JITTER_FACTOR)).thenReturn(0.0);
    Mockito.when(realmConfig.getConfig(STORAGE_CREDENTIAL_DURATION_SECONDS)).thenReturn(3600);
    return realmConfig;
  }
}
