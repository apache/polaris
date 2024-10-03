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
package org.apache.polaris.core.storage.gcp;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.auth.http.HttpTransportFactory;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.CredentialAccessBoundary;
import com.google.auth.oauth2.DownscopedCredentials;
import com.google.auth.oauth2.GoogleCredentials;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.storage.InMemoryStorageIntegration;
import org.apache.polaris.core.storage.PolarisCredentialProperty;
import org.apache.polaris.core.storage.PolarisStorageConfigurationInfo;
import org.apache.polaris.core.storage.PolarisStorageIntegration;
import org.apache.polaris.core.storage.StorageUtil;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * GCS implementation of {@link PolarisStorageIntegration} with support for scoping credentials for
 * input read/write locations
 */
public class GcpCredentialsStorageIntegration
    extends InMemoryStorageIntegration<GcpStorageConfigurationInfo> {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(GcpCredentialsStorageIntegration.class);

  private final GoogleCredentials sourceCredentials;
  private final HttpTransportFactory transportFactory;

  public GcpCredentialsStorageIntegration(
      GoogleCredentials sourceCredentials, HttpTransportFactory transportFactory) {
    super(GcpCredentialsStorageIntegration.class.getName());
    // Needed for when environment variable GOOGLE_APPLICATION_CREDENTIALS points to google service
    // account key json
    this.sourceCredentials =
        sourceCredentials.createScoped("https://www.googleapis.com/auth/cloud-platform");
    this.transportFactory = transportFactory;
  }

  @Override
  public EnumMap<PolarisCredentialProperty, String> getSubscopedCreds(
      @NotNull PolarisDiagnostics diagnostics,
      @NotNull GcpStorageConfigurationInfo storageConfig,
      boolean allowListOperation,
      @NotNull Set<String> allowedReadLocations,
      @NotNull Set<String> allowedWriteLocations) {
    try {
      sourceCredentials.refresh();
    } catch (IOException e) {
      throw new RuntimeException("Unable to refresh GCP credentials", e);
    }

    CredentialAccessBoundary accessBoundary =
        generateAccessBoundaryRules(
            allowListOperation, allowedReadLocations, allowedWriteLocations);
    DownscopedCredentials credentials =
        DownscopedCredentials.newBuilder()
            .setHttpTransportFactory(transportFactory)
            .setSourceCredential(sourceCredentials)
            .setCredentialAccessBoundary(accessBoundary)
            .build();
    AccessToken token;
    try {
      token = credentials.refreshAccessToken();
    } catch (IOException e) {
      LOGGER
          .atError()
          .addKeyValue("readLocations", allowedReadLocations)
          .addKeyValue("writeLocations", allowedWriteLocations)
          .addKeyValue("includesList", allowListOperation)
          .addKeyValue("accessBoundary", convertToString(accessBoundary))
          .log("Unable to refresh access credentials", e);
      throw new RuntimeException("Unable to fetch access credentials " + e.getMessage());
    }

    // If expires_in missing, use source credential's expire time, which require another api call to
    // get.
    EnumMap<PolarisCredentialProperty, String> propertyMap =
        new EnumMap<>(PolarisCredentialProperty.class);
    propertyMap.put(PolarisCredentialProperty.GCS_ACCESS_TOKEN, token.getTokenValue());
    propertyMap.put(
        PolarisCredentialProperty.GCS_ACCESS_TOKEN_EXPIRES_AT,
        String.valueOf(token.getExpirationTime().getTime()));
    return propertyMap;
  }

  private String convertToString(CredentialAccessBoundary accessBoundary) {
    try {
      return new ObjectMapper().writeValueAsString(accessBoundary);
    } catch (JsonProcessingException e) {
      LOGGER.warn("Unable to convert access boundary to json", e);
      return Objects.toString(accessBoundary);
    }
  }

  @VisibleForTesting
  public static CredentialAccessBoundary generateAccessBoundaryRules(
      boolean allowListOperation,
      @NotNull Set<String> allowedReadLocations,
      @NotNull Set<String> allowedWriteLocations) {
    Map<String, List<String>> readConditionsMap = new HashMap<>();
    Map<String, List<String>> writeConditionsMap = new HashMap<>();

    HashSet<String> readBuckets = new HashSet<>();
    HashSet<String> writeBuckets = new HashSet<>();
    Stream.concat(allowedReadLocations.stream(), allowedWriteLocations.stream())
        .distinct()
        .forEach(
            location -> {
              URI uri = URI.create(location);
              String bucket = StorageUtil.getBucket(uri);
              readBuckets.add(bucket);
              String path = uri.getPath().substring(1);
              List<String> resourceExpressions =
                  readConditionsMap.computeIfAbsent(bucket, key -> new ArrayList<>());
              resourceExpressions.add(
                  String.format(
                      "resource.name.startsWith('projects/_/buckets/%s/objects/%s')",
                      bucket, path));
              if (allowListOperation) {
                resourceExpressions.add(
                    String.format(
                        "api.getAttribute('storage.googleapis.com/objectListPrefix', '').startsWith('%s')",
                        path));
              }
              if (allowedWriteLocations.contains(location)) {
                writeBuckets.add(bucket);
                List<String> writeExpressions =
                    writeConditionsMap.computeIfAbsent(bucket, key -> new ArrayList<>());
                writeExpressions.add(
                    String.format(
                        "resource.name.startsWith('projects/_/buckets/%s/objects/%s')",
                        bucket, path));
              }
            });
    CredentialAccessBoundary.Builder accessBoundaryBuilder = CredentialAccessBoundary.newBuilder();
    readBuckets.forEach(
        bucket -> {
          List<String> readConditions = readConditionsMap.get(bucket);
          if (readConditions == null || readConditions.isEmpty()) {
            return;
          }
          CredentialAccessBoundary.AccessBoundaryRule.Builder builder =
              CredentialAccessBoundary.AccessBoundaryRule.newBuilder();
          builder.setAvailableResource(bucketResource(bucket));
          builder.setAvailabilityCondition(
              CredentialAccessBoundary.AccessBoundaryRule.AvailabilityCondition.newBuilder()
                  .setExpression(String.join(" || ", readConditions))
                  .build());
          builder.setAvailablePermissions(List.of("inRole:roles/storage.legacyObjectReader"));
          if (allowListOperation) {
            builder.addAvailablePermission("inRole:roles/storage.objectViewer");
          }
          accessBoundaryBuilder.addRule(builder.build());
        });
    writeBuckets.forEach(
        bucket -> {
          List<String> writeConditions = writeConditionsMap.get(bucket);
          if (writeConditions == null || writeConditions.isEmpty()) {
            return;
          }
          CredentialAccessBoundary.AccessBoundaryRule.Builder builder =
              CredentialAccessBoundary.AccessBoundaryRule.newBuilder();
          builder.setAvailableResource(bucketResource(bucket));
          builder.setAvailabilityCondition(
              CredentialAccessBoundary.AccessBoundaryRule.AvailabilityCondition.newBuilder()
                  .setExpression(String.join(" || ", writeConditions))
                  .build());
          builder.setAvailablePermissions(List.of("inRole:roles/storage.legacyBucketWriter"));
          accessBoundaryBuilder.addRule(builder.build());
        });
    return accessBoundaryBuilder.build();
  }

  private static String bucketResource(String bucket) {
    return "//storage.googleapis.com/projects/_/buckets/" + bucket;
  }

  @Override
  public EnumMap<PolarisStorageConfigurationInfo.DescribeProperty, String>
      descPolarisStorageConfiguration(@NotNull PolarisStorageConfigurationInfo storageConfigInfo) {
    return null;
  }
}
