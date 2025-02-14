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

import com.azure.core.credential.AccessToken;
import com.azure.core.credential.TokenRequestContext;
import com.azure.identity.DefaultAzureCredential;
import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.storage.blob.BlobContainerClientBuilder;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.models.UserDelegationKey;
import com.azure.storage.blob.sas.BlobSasPermission;
import com.azure.storage.blob.sas.BlobServiceSasSignatureValues;
import com.azure.storage.file.datalake.DataLakeFileSystemClientBuilder;
import com.azure.storage.file.datalake.DataLakeServiceClient;
import com.azure.storage.file.datalake.DataLakeServiceClientBuilder;
import com.azure.storage.file.datalake.models.DataLakeStorageException;
import com.azure.storage.file.datalake.sas.DataLakeServiceSasSignatureValues;
import com.azure.storage.file.datalake.sas.PathSasPermission;
import jakarta.annotation.Nonnull;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.Period;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import org.apache.polaris.core.PolarisConfiguration;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.storage.InMemoryStorageIntegration;
import org.apache.polaris.core.storage.PolarisCredentialProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

/** Azure credential vendor that supports generating SAS token */
public class AzureCredentialsStorageIntegration
    extends InMemoryStorageIntegration<AzureStorageConfigurationInfo> {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(AzureCredentialsStorageIntegration.class);

  final DefaultAzureCredential defaultAzureCredential;

  public AzureCredentialsStorageIntegration() {
    super(AzureCredentialsStorageIntegration.class.getName());
    // The DefaultAzureCredential will by default load the environment variables for client id,
    // client secret, tenant id
    defaultAzureCredential = new DefaultAzureCredentialBuilder().build();
  }

  @Override
  public EnumMap<PolarisCredentialProperty, String> getSubscopedCreds(
      @Nonnull PolarisDiagnostics diagnostics,
      @Nonnull AzureStorageConfigurationInfo storageConfig,
      boolean allowListOperation,
      @Nonnull Set<String> allowedReadLocations,
      @Nonnull Set<String> allowedWriteLocations) {
    EnumMap<PolarisCredentialProperty, String> credentialMap =
        new EnumMap<>(PolarisCredentialProperty.class);
    String loc =
        !allowedWriteLocations.isEmpty()
            ? allowedWriteLocations.stream().findAny().orElse(null)
            : allowedReadLocations.stream().findAny().orElse(null);
    if (loc == null) {
      throw new IllegalArgumentException("Expect valid location");
    }
    // schema://<container_name>@<account_name>.<endpoint>/<file_path>
    AzureLocation location = new AzureLocation(loc);
    validateAccountAndContainer(location, allowedReadLocations, allowedWriteLocations);

    String storageDnsName = location.getStorageAccount() + "." + location.getEndpoint();
    String filePath = location.getFilePath();

    BlobSasPermission blobSasPermission = new BlobSasPermission();
    // pathSasPermission is for Data lake storage
    PathSasPermission pathSasPermission = new PathSasPermission();

    if (allowListOperation) {
      // container level
      blobSasPermission.setListPermission(true);
      pathSasPermission.setListPermission(true);
    }
    if (!allowedReadLocations.isEmpty()) {
      blobSasPermission.setReadPermission(true);
      pathSasPermission.setReadPermission(true);
    }
    if (!allowedWriteLocations.isEmpty()) {
      blobSasPermission.setAddPermission(true);
      blobSasPermission.setWritePermission(true);
      blobSasPermission.setDeletePermission(true);
      pathSasPermission.setAddPermission(true);
      pathSasPermission.setWritePermission(true);
      pathSasPermission.setDeletePermission(true);
    }

    Instant start = Instant.now();
    OffsetDateTime expiry =
        OffsetDateTime.ofInstant(
            start.plusSeconds(3600), ZoneOffset.UTC); // 1 hr to sync with AWS and GCP Access token

    AccessToken accessToken = getAccessToken(storageConfig.getTenantId());
    // Get user delegation key.
    // Set the new generated user delegation key expiry to 7 days and minute 1 min
    // Azure strictly requires the end time to be <= 7 days from the current time, -1 min to avoid
    // clock skew between the client and server,
    OffsetDateTime startTime = start.truncatedTo(ChronoUnit.SECONDS).atOffset(ZoneOffset.UTC);
    int intendedDurationSeconds =
        PolarisConfiguration.loadConfig(PolarisConfiguration.STORAGE_CREDENTIAL_DURATION_SECONDS);
    OffsetDateTime intendedEndTime =
        start.plusSeconds(intendedDurationSeconds).atOffset(ZoneOffset.UTC);
    OffsetDateTime maxAllowedEndTime =
        start.plus(Period.ofDays(7)).minusSeconds(60).atOffset(ZoneOffset.UTC);
    OffsetDateTime sanitizedEndTime =
        intendedEndTime.isBefore(maxAllowedEndTime) ? intendedEndTime : maxAllowedEndTime;

    LOGGER
        .atDebug()
        .addKeyValue("allowedListAction", allowListOperation)
        .addKeyValue("allowedReadLoc", allowedReadLocations)
        .addKeyValue("allowedWriteLoc", allowedWriteLocations)
        .addKeyValue("location", loc)
        .addKeyValue("storageAccount", location.getStorageAccount())
        .addKeyValue("endpoint", location.getEndpoint())
        .addKeyValue("container", location.getContainer())
        .addKeyValue("filePath", filePath)
        .log("Subscope Azure SAS");
    String sasToken = "";
    if (location.getEndpoint().equalsIgnoreCase(AzureLocation.BLOB_ENDPOINT)) {
      sasToken =
          getBlobUserDelegationSas(
              startTime,
              sanitizedEndTime,
              expiry,
              storageDnsName,
              location.getContainer(),
              blobSasPermission,
              Mono.just(accessToken));
    } else if (location.getEndpoint().equalsIgnoreCase(AzureLocation.ADLS_ENDPOINT)) {
      sasToken =
          getAdlsUserDelegationSas(
              startTime,
              sanitizedEndTime,
              expiry,
              storageDnsName,
              location.getContainer(),
              pathSasPermission,
              Mono.just(accessToken));
    } else {
      throw new RuntimeException(
          String.format("Endpoint %s not supported", location.getEndpoint()));
    }
    credentialMap.put(PolarisCredentialProperty.AZURE_SAS_TOKEN, sasToken);
    credentialMap.put(PolarisCredentialProperty.AZURE_ACCOUNT_HOST, storageDnsName);
    credentialMap.put(
        PolarisCredentialProperty.EXPIRATION_TIME,
        String.valueOf(sanitizedEndTime.toInstant().toEpochMilli()));
    return credentialMap;
  }

  private String getBlobUserDelegationSas(
      OffsetDateTime startTime,
      OffsetDateTime keyEndtime,
      OffsetDateTime sasExpiry,
      String storageDnsName,
      String container,
      BlobSasPermission blobSasPermission,
      Mono<AccessToken> accessTokenMono) {
    String endpoint = "https://" + storageDnsName;
    try {
      BlobServiceClient serviceClient =
          new BlobServiceClientBuilder()
              .endpoint(endpoint)
              .credential(c -> accessTokenMono)
              .buildClient();
      UserDelegationKey userDelegationKey =
          serviceClient.getUserDelegationKey(startTime, keyEndtime);
      BlobServiceSasSignatureValues sigValues =
          new BlobServiceSasSignatureValues(sasExpiry, blobSasPermission);
      // scoped to the container
      return new BlobContainerClientBuilder()
          .endpoint(endpoint)
          .containerName(container)
          .buildClient()
          .generateUserDelegationSas(sigValues, userDelegationKey);
    } catch (BlobStorageException ex) {
      LOGGER.debug(
          "Azure DataLakeStorageException for getBlobUserDelegationSas. keyStart={} keyEnd={}, storageDns={}, container={}",
          startTime,
          keyEndtime,
          storageDnsName,
          container,
          ex);
      throw ex;
    }
  }

  private String getAdlsUserDelegationSas(
      OffsetDateTime startTime,
      OffsetDateTime endTime,
      OffsetDateTime sasExpiry,
      String storageDnsName,
      String fileSystemNameOrContainer,
      PathSasPermission pathSasPermission,
      Mono<AccessToken> accessTokenMono) {
    String endpoint = "https://" + storageDnsName;
    try {
      DataLakeServiceClient dataLakeServiceClient =
          new DataLakeServiceClientBuilder()
              .endpoint(endpoint)
              .credential(c -> accessTokenMono)
              .buildClient();
      com.azure.storage.file.datalake.models.UserDelegationKey userDelegationKey =
          dataLakeServiceClient.getUserDelegationKey(startTime, endTime);

      DataLakeServiceSasSignatureValues signatureValues =
          new DataLakeServiceSasSignatureValues(sasExpiry, pathSasPermission);

      return new DataLakeFileSystemClientBuilder()
          .endpoint(endpoint)
          .fileSystemName(fileSystemNameOrContainer)
          .buildClient()
          .generateUserDelegationSas(signatureValues, userDelegationKey);
    } catch (DataLakeStorageException ex) {
      LOGGER.debug(
          "Azure DataLakeStorageException for getAdlsUserDelegationSas. keyStart={} keyEnd={}, storageDns={}, fileSystemName={}",
          startTime,
          endTime,
          storageDnsName,
          fileSystemNameOrContainer,
          ex);
      throw ex;
    }
  }

  /** Verify that storage accounts, containers and endpoint are the same */
  private void validateAccountAndContainer(
      AzureLocation target, Set<String> readLocations, Set<String> writeLocations) {
    Set<String> allLocations = new HashSet<>();
    allLocations.addAll(readLocations);
    allLocations.addAll(writeLocations);
    allLocations.forEach(
        loc -> {
          AzureLocation location = new AzureLocation(loc);
          if (!Objects.equals(location.getStorageAccount(), target.getStorageAccount())
              || !Objects.equals(location.getContainer(), target.getContainer())
              || !Objects.equals(location.getEndpoint(), target.getEndpoint())) {
            throw new RuntimeException(
                "Expect allowed read write locations belong to the same storage account and container");
          }
        });
  }

  private AccessToken getAccessToken(String tenantId) {
    String scope = "https://storage.azure.com/.default";
    AccessToken accessToken =
        defaultAzureCredential
            .getToken(new TokenRequestContext().addScopes(scope).setTenantId(tenantId))
            .blockOptional()
            .orElse(null);
    if (accessToken == null) {
      throw new RuntimeException("No access token fetched!");
    }
    return accessToken;
  }
}
