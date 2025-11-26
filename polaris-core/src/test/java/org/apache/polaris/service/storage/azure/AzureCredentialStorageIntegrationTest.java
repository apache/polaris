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
package org.apache.polaris.service.storage.azure;

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobClientBuilder;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.models.ListBlobsOptions;
import com.azure.storage.blob.options.BlobParallelUploadOptions;
import com.azure.storage.common.Utility;
import com.azure.storage.file.datalake.DataLakeFileClient;
import com.azure.storage.file.datalake.DataLakeFileSystemClient;
import com.azure.storage.file.datalake.DataLakeFileSystemClientBuilder;
import com.azure.storage.file.datalake.models.DataLakeStorageException;
import com.azure.storage.file.datalake.models.PathItem;
import com.google.common.base.Strings;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;
import org.apache.polaris.core.storage.BaseStorageIntegrationTest;
import org.apache.polaris.core.storage.StorageAccessConfig;
import org.apache.polaris.core.storage.StorageAccessProperty;
import org.apache.polaris.core.storage.azure.AzureCredentialsStorageIntegration;
import org.apache.polaris.core.storage.azure.AzureStorageConfigurationInfo;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.junit.jupiter.params.support.ParameterDeclarations;

public class AzureCredentialStorageIntegrationTest extends BaseStorageIntegrationTest {

  private final String clientId = System.getenv("AZURE_CLIENT_ID");
  private final String clientSecret = System.getenv("AZURE_CLIENT_SECRET");
  private final String tenantId = System.getenv("AZURE_TENANT_ID");

  private void assumeEnvVariablesNotNull() {
    Assumptions.assumeThat(
            Strings.isNullOrEmpty(clientId)
                || Strings.isNullOrEmpty(clientSecret)
                || Strings.isNullOrEmpty(tenantId))
        .describedAs("Null Azure testing environment variables!")
        .isFalse();
  }

  @Test
  public void testNegativeCases() {
    List<String> differentEndpointList =
        Arrays.asList(
            "abfss://container@icebergdfsstorageacct.dfs.core.windows.net/polaris-test/",
            "abfss://container@icebergdfsstorageacct.blob.core.windows.net/polaris-test/");
    Assertions.assertThatThrownBy(
            () ->
                subscopedCredsForOperations(
                    differentEndpointList, /* allowedWriteLoc= */ new ArrayList<>(), true))
        .isInstanceOf(RuntimeException.class);

    List<String> differentStorageAccts =
        Arrays.asList(
            "abfss://container@polarisadls.dfs.core.windows.net/polaris-test/",
            "abfss://container@icebergdfsstorageacct.dfs.core.windows.net/polaris-test/");
    Assertions.assertThatThrownBy(
            () ->
                subscopedCredsForOperations(
                    differentStorageAccts, /* allowedWriteLoc= */ new ArrayList<>(), true))
        .isInstanceOf(RuntimeException.class);
    List<String> differentContainers =
        Arrays.asList(
            "abfss://container1@icebergdfsstorageacct.dfs.core.windows.net/polaris-test/",
            "abfss://container2@icebergdfsstorageacct.dfs.core.windows.net/polaris-test/");

    Assertions.assertThatThrownBy(
            () ->
                subscopedCredsForOperations(
                    differentContainers, /* allowedWriteLoc= */ new ArrayList<>(), true))
        .isInstanceOf(RuntimeException.class);
  }

  @TestWithAzureArgs
  public void testGetSubscopedTokenList(boolean allowListAction, String service) {

    assumeEnvVariablesNotNull();

    boolean isBlobService = service.equals("blob");
    List<String> allowedLoc =
        List.of(
            String.format(
                "abfss://container@icebergdfsstorageacct.%s.core.windows.net/polaris-test/",
                service));
    StorageAccessConfig storageAccessConfig =
        subscopedCredsForOperations(
            /* allowedReadLoc= */ allowedLoc,
            /* allowedWriteLoc= */ new ArrayList<>(),
            allowListAction);
    Assertions.assertThat(storageAccessConfig.credentials()).hasSize(2);
    String sasToken = storageAccessConfig.get(StorageAccessProperty.AZURE_SAS_TOKEN);
    Assertions.assertThat(sasToken).isNotNull();
    String serviceEndpoint =
        String.format("https://icebergdfsstorageacct.%s.core.windows.net", service);
    BlobContainerClient containerClient =
        createContainerClient(sasToken, serviceEndpoint, "container");
    DataLakeFileSystemClient fileSystemClient =
        createDatalakeFileSystemClient(sasToken, serviceEndpoint, "container");

    if (allowListAction) {
      // LIST succeed
      Assertions.assertThatNoException()
          .isThrownBy(
              () -> {
                if (isBlobService) {
                  containerClient
                      .listBlobs(
                          new ListBlobsOptions().setPrefix(Utility.urlEncode("polaris-test/")),
                          Duration.ofSeconds(5))
                      .streamByPage()
                      .findFirst()
                      .orElse(null);
                } else {
                  fileSystemClient
                      .getDirectoryClient("polaris-test")
                      .listPaths()
                      .forEach(PathItem::getName);
                }
              });
    } else {
      if (isBlobService) {
        Assertions.assertThatThrownBy(
                () ->
                    containerClient
                        .listBlobs(
                            new ListBlobsOptions().setPrefix(Utility.urlEncode("polaris-test/")),
                            Duration.ofSeconds(5))
                        .streamByPage()
                        .findFirst()
                        .orElse(null))
            .isInstanceOf(BlobStorageException.class);
      } else {
        Assertions.assertThatThrownBy(
                () ->
                    fileSystemClient
                        .getDirectoryClient("polaris-test")
                        .listPaths()
                        .forEach(PathItem::getName))
            .isInstanceOf(DataLakeStorageException.class);
      }
    }
  }

  @TestWithAzureArgs
  public void testGetSubscopedTokenRead(
      @SuppressWarnings("unused") boolean allowListAction, String service) {
    assumeEnvVariablesNotNull();

    String allowedPrefix = "polaris-test";
    String blockedPrefix = "blocked-prefix";
    List<String> allowedLoc =
        List.of(
            String.format(
                "abfss://container@icebergdfsstorageacct.%s.core.windows.net/%s",
                service, allowedPrefix));
    StorageAccessConfig storageAccessConfig =
        subscopedCredsForOperations(
            /* allowedReadLoc= */ allowedLoc,
            /* allowedWriteLoc= */ new ArrayList<>(),
            /* allowListAction= */ false);

    BlobClient blobClient =
        createBlobClient(
            storageAccessConfig.get(StorageAccessProperty.AZURE_SAS_TOKEN),
            "https://icebergdfsstorageacct.dfs.core.windows.net",
            "container",
            allowedPrefix);

    // READ succeed
    Assertions.assertThatNoException()
        .isThrownBy(
            () ->
                blobClient.downloadStreamWithResponse(
                    new ByteArrayOutputStream(),
                    null,
                    null,
                    null,
                    false,
                    Duration.ofSeconds(5),
                    null));

    // read will fail because only READ permission allowed
    Assertions.assertThatThrownBy(
            () ->
                blobClient.uploadWithResponse(
                    new BlobParallelUploadOptions(
                        new ByteArrayInputStream("polaris".getBytes(StandardCharsets.UTF_8))),
                    Duration.ofSeconds(5),
                    null))
        .isInstanceOf(BlobStorageException.class);

    // read fail because container is blocked
    BlobClient blobClientReadFail =
        createBlobClient(
            storageAccessConfig.get(StorageAccessProperty.AZURE_SAS_TOKEN),
            String.format("https://icebergdfsstorageacct.%s.core.windows.net", service),
            "regtest",
            blockedPrefix);

    Assertions.assertThatThrownBy(
            () ->
                blobClientReadFail.downloadStreamWithResponse(
                    new ByteArrayOutputStream(),
                    null,
                    null,
                    null,
                    false,
                    Duration.ofSeconds(5),
                    null))
        .isInstanceOf(BlobStorageException.class);
  }

  @TestWithAzureArgs
  public void testGetSubscopedTokenWrite(
      @SuppressWarnings("unused") boolean allowListAction, String service) {
    assumeEnvVariablesNotNull();

    boolean isBlobService = service.equals("blob");
    String allowedPrefix = "polaris-test/scopedcreds/";
    String blockedPrefix = "blocked-prefix";
    List<String> allowedLoc =
        List.of(
            String.format(
                "abfss://container@icebergdfsstorageacct.%s.core.windows.net/%s",
                service, allowedPrefix));
    StorageAccessConfig storageAccessConfig =
        subscopedCredsForOperations(
            /* allowedReadLoc= */ new ArrayList<>(),
            /* allowedWriteLoc= */ allowedLoc,
            /* allowListAction= */ false);
    String serviceEndpoint =
        String.format("https://icebergdfsstorageacct.%s.core.windows.net", service);
    BlobClient blobClient =
        createBlobClient(
            storageAccessConfig.get(StorageAccessProperty.AZURE_SAS_TOKEN),
            serviceEndpoint,
            "container",
            allowedPrefix + "metadata/00000-65ffa17b-fe64-4c38-bcb9-06f9bd12aa2a.metadata.json");
    DataLakeFileClient fileClient =
        createDatalakeFileClient(
            storageAccessConfig.get(StorageAccessProperty.AZURE_SAS_TOKEN),
            serviceEndpoint,
            "container",
            "polaris-test/scopedcreds/metadata",
            "00000-65ffa17b-fe64-4c38-bcb9-06f9bd12aa2a.metadata.json");
    // upload succeed
    ByteArrayInputStream inputStream =
        new ByteArrayInputStream("polaris".getBytes(StandardCharsets.UTF_8));
    Assertions.assertThatNoException()
        .isThrownBy(
            () -> {
              if (isBlobService) {
                blobClient.uploadWithResponse(
                    new BlobParallelUploadOptions(inputStream), Duration.ofSeconds(5), null);
              } else {
                fileClient.upload(inputStream, "polaris".length(), /*override*/ true);
              }
            });
    ByteArrayOutputStream outStream = new ByteArrayOutputStream();
    // READ not allowed
    if (isBlobService) {
      Assertions.assertThatThrownBy(
              () ->
                  blobClient.downloadStreamWithResponse(
                      outStream, null, null, null, false, Duration.ofSeconds(5), null))
          .isInstanceOf(BlobStorageException.class);
    } else {
      Assertions.assertThatThrownBy(() -> fileClient.read(outStream))
          .isInstanceOf(DataLakeStorageException.class);
    }

    // upload fail because container not allowed
    String blockedContainer = "regtest";
    BlobClient blobClientWriteFail =
        createBlobClient(
            storageAccessConfig.get(StorageAccessProperty.AZURE_SAS_TOKEN),
            serviceEndpoint,
            blockedContainer,
            blockedPrefix);
    DataLakeFileClient fileClientFail =
        createDatalakeFileClient(
            storageAccessConfig.get(StorageAccessProperty.AZURE_SAS_TOKEN),
            serviceEndpoint,
            blockedContainer,
            "polaris-test/scopedcreds/metadata",
            "00000-65ffa17b-fe64-4c38-bcb9-06f9bd12aa2a.metadata.json");

    if (isBlobService) {
      Assertions.assertThatThrownBy(
              () ->
                  blobClientWriteFail.uploadWithResponse(
                      new BlobParallelUploadOptions(
                          new ByteArrayInputStream("polaris".getBytes(StandardCharsets.UTF_8))),
                      Duration.ofSeconds(5),
                      null))
          .isInstanceOf(BlobStorageException.class);
    } else {
      Assertions.assertThatThrownBy(() -> fileClientFail.upload(inputStream, "polaris".length()))
          .isInstanceOf(DataLakeStorageException.class);
    }
  }

  private StorageAccessConfig subscopedCredsForOperations(
      List<String> allowedReadLoc, List<String> allowedWriteLoc, boolean allowListAction) {
    AzureStorageConfigurationInfo azureConfig =
        AzureStorageConfigurationInfo.builder()
            .addAllAllowedLocations(allowedReadLoc)
            .addAllAllowedLocations(allowedWriteLoc)
            .tenantId(tenantId)
            .build();
    AzureCredentialsStorageIntegration azureCredsIntegration =
        new AzureCredentialsStorageIntegration(azureConfig);
    return azureCredsIntegration.getSubscopedCreds(
        EMPTY_REALM_CONFIG,
        allowListAction,
        new HashSet<>(allowedReadLoc),
        new HashSet<>(allowedWriteLoc),
        Optional.empty());
  }

  private BlobContainerClient createContainerClient(
      String sasToken, String endpoint, String container) {
    BlobServiceClient blobServiceClient =
        new BlobServiceClientBuilder().sasToken(sasToken).endpoint(endpoint).buildClient();
    return blobServiceClient.getBlobContainerClient(container);
  }

  private DataLakeFileSystemClient createDatalakeFileSystemClient(
      String sasToken, String endpoint, String containerOrFileSystem) {
    return new DataLakeFileSystemClientBuilder()
        .sasToken(sasToken)
        .endpoint(endpoint)
        .fileSystemName(containerOrFileSystem)
        .buildClient();
  }

  private BlobClient createBlobClient(
      String sasToken, String endpoint, String container, String filePath) {
    BlobServiceClient blobServiceClient =
        new BlobServiceClientBuilder().sasToken(sasToken).endpoint(endpoint).buildClient();
    return new BlobClientBuilder()
        .endpoint(blobServiceClient.getAccountUrl())
        .pipeline(blobServiceClient.getHttpPipeline())
        .containerName(container)
        .blobName(filePath)
        .buildClient();
  }

  private DataLakeFileClient createDatalakeFileClient(
      String sasToken,
      String endpoint,
      String containerOrFileSystem,
      String directory,
      String fileName) {
    DataLakeFileSystemClient dataLakeFileSystemClient =
        createDatalakeFileSystemClient(sasToken, endpoint, containerOrFileSystem);
    return dataLakeFileSystemClient.getDirectoryClient(directory).getFileClient(fileName);
  }

  @Target({ElementType.METHOD})
  @Retention(RetentionPolicy.RUNTIME)
  @ParameterizedTest
  @ArgumentsSource(AzureTestArgs.class)
  protected @interface TestWithAzureArgs {}

  protected static class AzureTestArgs implements ArgumentsProvider {
    @Override
    public Stream<? extends Arguments> provideArguments(
        ParameterDeclarations parameterDeclarations, ExtensionContext extensionContext) {
      return Stream.of(
          Arguments.of(/* allowedList= */ true, "blob"),
          Arguments.of(/* allowedList= */ false, "blob"),
          Arguments.of(/* allowedList= */ true, "dfs"),
          Arguments.of(/* allowedList= */ false, "dfs"));
    }
  }
}
