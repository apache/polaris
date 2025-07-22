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
package org.apache.polaris.core.storage.oss;

import static org.apache.polaris.core.config.FeatureConfiguration.STORAGE_CREDENTIAL_DURATION_SECONDS;

import com.aliyuncs.AcsRequest;
import com.aliyuncs.AcsResponse;
import com.aliyuncs.DefaultAcsClient;
import com.aliyuncs.IAcsClient;
import com.aliyuncs.profile.DefaultProfile;
import com.aliyuncs.auth.sts.AssumeRoleRequest;
import com.aliyuncs.auth.sts.AssumeRoleResponse;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.Nonnull;
import java.net.URI;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.storage.InMemoryStorageIntegration;
import org.apache.polaris.core.storage.PolarisStorageActions;
import org.apache.polaris.core.storage.StorageAccessProperty;
import org.apache.polaris.core.storage.StorageUtil;
import org.apache.polaris.core.storage.oss.OssStsClientProvider.StsDestination;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** OSS credential vendor that supports generating temporary credentials using Alibaba Cloud STS */
public class OssCredentialsStorageIntegration
    extends InMemoryStorageIntegration<OssStorageConfigurationInfo> {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(OssCredentialsStorageIntegration.class);

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private final OssStsClientProvider stsClientProvider;

  public OssCredentialsStorageIntegration() {
    this(new OssStsClientProvider());
  }


  public OssCredentialsStorageIntegration(OssStsClientProvider stsClientProvider) {
    super(OssCredentialsStorageIntegration.class.getName());
    this.stsClientProvider = stsClientProvider;
  }

  /** {@inheritDoc} */
  @Override
  public EnumMap<StorageAccessProperty, String> getSubscopedCreds(
      @Nonnull CallContext callContext,
      @Nonnull OssStorageConfigurationInfo storageConfig,
      boolean allowListOperation,
      @Nonnull Set<String> allowedReadLocations,
      @Nonnull Set<String> allowedWriteLocations) {

    int storageCredentialDurationSeconds =
        callContext.getRealmConfig().getConfig(STORAGE_CREDENTIAL_DURATION_SECONDS);

    EnumMap<StorageAccessProperty, String> credentialMap =
        new EnumMap<>(StorageAccessProperty.class);

    try {
      // Generate role session name
      String roleSessionName = "PolarisOssCredentialsStorageIntegration-" + System.currentTimeMillis();

      // Generate RAM policy for scoped access
      String policy = generateOssPolicy(allowListOperation, allowedReadLocations, allowedWriteLocations);

      // Use STS client provider to assume role directly
      AssumeRoleResponse response = stsClientProvider.assumeRole(
          storageConfig.getRoleArn(),
          roleSessionName,
          storageConfig.getExternalId(),
          storageConfig.getRegion(),
          storageConfig.getStsEndpointUri(),
          policy,
          (long) storageCredentialDurationSeconds);

      AssumeRoleResponse.Credentials credentials = response.getCredentials();

      // Populate credential map with STS response
      credentialMap.put(StorageAccessProperty.OSS_ACCESS_KEY_ID, credentials.getAccessKeyId());
      credentialMap.put(StorageAccessProperty.OSS_ACCESS_KEY_SECRET, credentials.getAccessKeySecret());
      credentialMap.put(StorageAccessProperty.OSS_SECURITY_TOKEN, credentials.getSecurityToken());

      // 设置为OSS模式的特殊属性，帮助Apache Iceberg识别这是OSS凭证
      credentialMap.put(StorageAccessProperty.CLIENT_REGION, storageConfig.getRegion() != null ? storageConfig.getRegion() : "cn-hangzhou");

      // Set expiration time - convert ISO format to milliseconds timestamp
      String expirationStr = credentials.getExpiration();
      try {
        Instant expirationInstant = Instant.parse(expirationStr);
        long expirationMillis = expirationInstant.toEpochMilli();
        credentialMap.put(StorageAccessProperty.EXPIRATION_TIME, String.valueOf(expirationMillis));
      } catch (Exception e) {
        LOGGER.warn("Failed to parse expiration time '{}', using current time + 1 hour", expirationStr, e);
        // Fallback to current time + 1 hour if parsing fails
        long fallbackExpiration = System.currentTimeMillis() + 3600_000;
        // 1 hour
        credentialMap.put(StorageAccessProperty.EXPIRATION_TIME, String.valueOf(fallbackExpiration));
      }

      // Set region if provided
      if (storageConfig.getRegion() != null) {
        credentialMap.put(StorageAccessProperty.OSS_REGION, storageConfig.getRegion());
      }

      // Set endpoint if provided
      URI endpointUri = storageConfig.getEndpointUri();
      if (endpointUri != null) {
        credentialMap.put(StorageAccessProperty.OSS_ENDPOINT, endpointUri.toString());
      }

      LOGGER.debug(
          "Generated OSS subscoped credentials for roleArn={}, region={}, duration={}s",
          storageConfig.getRoleArn(),
          storageConfig.getRegion(),
          storageCredentialDurationSeconds);

    } catch (Exception e) {
      LOGGER.error("Failed to assume OSS role: {}", e.getMessage(), e);
      throw new RuntimeException("Failed to assume OSS role", e);
    }

    return credentialMap;
  }

  /**
   * Generate OSS RAM policy based on allowed operations and locations
   * This follows Alibaba Cloud RAM policy format
   */
  private String generateOssPolicy(boolean allowList, Set<String> readLocations, Set<String> writeLocations) {
    if (readLocations.isEmpty() && writeLocations.isEmpty()) {
      return null;
      // Use role's default permissions
    }

    try {
      RamPolicy policy = new RamPolicy();
      policy.version = "1";
      policy.statement = new ArrayList<>();

      // Collect all unique buckets
      Set<String> allBuckets = Stream.concat(readLocations.stream(), writeLocations.stream())
          .map(this::extractBucketFromOssLocation)
          .collect(Collectors.toSet());

      // Add GetBucketLocation permission for all buckets
      if (!allBuckets.isEmpty()) {
        RamStatement getBucketLocationStatement = new RamStatement();
        getBucketLocationStatement.effect = "Allow";
        getBucketLocationStatement.action = Arrays.asList("oss:GetBucketLocation");
        getBucketLocationStatement.resource = allBuckets.stream()
            .map(bucket -> "acs:oss:*:*:" + bucket)
            .collect(Collectors.toList());
        policy.statement.add(getBucketLocationStatement);
      }

      // Add read permissions
      if (!readLocations.isEmpty()) {
        RamStatement readStatement = new RamStatement();
        readStatement.effect = "Allow";
        readStatement.action = Arrays.asList("oss:GetObject", "oss:GetObjectVersion");
        readStatement.resource = readLocations.stream()
            .map(location -> convertLocationToRamResource(location, "*"))
            .collect(Collectors.toList());
        policy.statement.add(readStatement);

        // Add list permissions if allowed
        if (allowList) {
          RamStatement listStatement = new RamStatement();
          listStatement.effect = "Allow";
          listStatement.action = Arrays.asList("oss:ListObjects", "oss:ListObjectsV2");

          // Group by bucket for condition-based listing
          Map<String, List<String>> bucketToPathsMap = readLocations.stream()
              .collect(Collectors.groupingBy(
                  this::extractBucketFromOssLocation,
                  Collectors.mapping(this::extractPathFromOssLocation, Collectors.toList())
              ));

          listStatement.resource = bucketToPathsMap.keySet().stream()
              .map(bucket -> "acs:oss:*:*:" + bucket)
              .collect(Collectors.toList());

          // Add conditions for path-based listing
          if (bucketToPathsMap.values().stream().anyMatch(paths -> paths.stream().anyMatch(path -> !path.isEmpty()))) {
            RamCondition condition = new RamCondition();
            condition.stringLike = new HashMap<>();

            List<String> prefixes = bucketToPathsMap.values().stream()
                .flatMap(List::stream)
                .filter(path -> !path.isEmpty())
                .map(path -> path.endsWith("/") ? path + "*" : path + "/*")
                .collect(Collectors.toList());

            condition.stringLike.put("oss:Prefix", prefixes);
            listStatement.condition = condition;
          }

          policy.statement.add(listStatement);
        }
      }

      // Add write permissions
      if (!writeLocations.isEmpty()) {
        RamStatement writeStatement = new RamStatement();
        writeStatement.effect = "Allow";
        writeStatement.action = Arrays.asList("oss:PutObject", "oss:DeleteObject");
        writeStatement.resource = writeLocations.stream()
            .map(location -> convertLocationToRamResource(location, "*"))
            .collect(Collectors.toList());
        policy.statement.add(writeStatement);
      }

      String policyJson = OBJECT_MAPPER.writeValueAsString(policy);
      LOGGER.debug("Generated OSS RAM policy: {}", policyJson);
      return policyJson;

    } catch (JsonProcessingException e) {
      LOGGER.error("Failed to generate OSS RAM policy", e);
      return null;
    }
  }

  private String extractBucketFromOssLocation(String location) {
    OssLocation ossLocation = new OssLocation(location);
    return ossLocation.getBucket();
  }

  private String extractPathFromOssLocation(String location) {
    OssLocation ossLocation = new OssLocation(location);
    return ossLocation.getKey();
  }

  private String convertLocationToRamResource(String location, String suffix) {
    OssLocation ossLocation = new OssLocation(location);
    String path = ossLocation.getKey();
    if (path.isEmpty()) {
      return "acs:oss:*:*:" + ossLocation.getBucket() + "/*";
    }
    return "acs:oss:*:*:" + ossLocation.getBucket() + "/" +
           StorageUtil.concatFilePrefixes(path, suffix, "/");
  }

  @Override
  public @Nonnull Map<String, Map<PolarisStorageActions, ValidationResult>>
      validateAccessToLocations(
          @Nonnull OssStorageConfigurationInfo storageConfig,
          @Nonnull Set<PolarisStorageActions> actions,
          @Nonnull Set<String> locations) {

    Map<String, Map<PolarisStorageActions, ValidationResult>> result = new HashMap<>();

    // For each location, validate against allowed locations in storage config
    for (String location : locations) {
      Map<PolarisStorageActions, ValidationResult> locationResult = new HashMap<>();

      boolean isLocationAllowed = storageConfig.getAllowedLocations().stream()
          .anyMatch(allowedLocation -> {
            try {
              OssLocation ossLocation = new OssLocation(location);
              OssLocation allowedOssLocation = new OssLocation(allowedLocation);
              return ossLocation.isChildOf(allowedOssLocation);
            } catch (Exception e) {
              LOGGER.warn("Failed to validate location: {}", location, e);
              return false;
            }
          });

      for (PolarisStorageActions action : actions) {
        ValidationResult validationResult = new ValidationResult(
            isLocationAllowed,
            isLocationAllowed ? "" : "Location not in allowed list: " + location
        );
        locationResult.put(action, validationResult);
      }

      result.put(location, locationResult);
    }

    LOGGER.debug("OSS access validation completed for locations={}, actions={}", locations, actions);
    return result;
  }

  // RAM Policy data structures for JSON serialization
  @JsonInclude(JsonInclude.Include.NON_NULL)
  private static class RamPolicy {
    @JsonProperty("Version")
    public String version;

    @JsonProperty("Statement")
    public List<RamStatement> statement;
  }

  @JsonInclude(JsonInclude.Include.NON_NULL)
  private static class RamStatement {
    @JsonProperty("Effect")
    public String effect;

    @JsonProperty("Action")
    public List<String> action;

    @JsonProperty("Resource")
    public List<String> resource;

    @JsonProperty("Condition")
    public RamCondition condition;
  }

  @JsonInclude(JsonInclude.Include.NON_NULL)
  private static class RamCondition {
    @JsonProperty("StringLike")
    public Map<String, List<String>> stringLike;
  }
}