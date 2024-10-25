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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.polaris.core.PolarisConfiguration;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.admin.model.Catalog;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.entity.CatalogEntity;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PolarisEntityConstants;
import org.apache.polaris.core.entity.TableLikeEntity;
import org.apache.polaris.core.storage.aws.AwsStorageConfigurationInfo;
import org.apache.polaris.core.storage.azure.AzureStorageConfigurationInfo;
import org.apache.polaris.core.storage.gcp.GcpStorageConfigurationInfo;
import org.apache.polaris.core.storage.s3compatible.S3CompatibleStorageConfigurationInfo;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The polaris storage configuration information, is part of a polaris entity's internal property,
 * that holds necessary information including
 *
 * <pre>
 * 1. locations that allows polaris to get access to
 * 2. cloud identity info that a service principle can request access token to the locations
 * </pre>
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME)
@JsonSubTypes({
  @JsonSubTypes.Type(value = AwsStorageConfigurationInfo.class),
  @JsonSubTypes.Type(value = S3CompatibleStorageConfigurationInfo.class),
  @JsonSubTypes.Type(value = AzureStorageConfigurationInfo.class),
  @JsonSubTypes.Type(value = GcpStorageConfigurationInfo.class),
  @JsonSubTypes.Type(value = FileStorageConfigurationInfo.class),
})
public abstract class PolarisStorageConfigurationInfo {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(PolarisStorageConfigurationInfo.class);

  // a list of allowed locations
  private final List<String> allowedLocations;

  // storage type
  private final StorageType storageType;

  public PolarisStorageConfigurationInfo(
      @JsonProperty(value = "storageType", required = true) @NotNull StorageType storageType,
      @JsonProperty(value = "allowedLocations", required = true) @NotNull
          List<String> allowedLocations) {
    this(storageType, allowedLocations, true);
  }

  protected PolarisStorageConfigurationInfo(
      StorageType storageType, List<String> allowedLocations, boolean validatePrefix) {
    this.allowedLocations = allowedLocations;
    this.storageType = storageType;
    if (validatePrefix) {
      allowedLocations.forEach(this::validatePrefixForStorageType);
    }
  }

  public List<String> getAllowedLocations() {
    return allowedLocations;
  }

  public StorageType getStorageType() {
    return storageType;
  }

  private static final ObjectMapper DEFAULT_MAPPER;

  static {
    DEFAULT_MAPPER = new ObjectMapper();
    DEFAULT_MAPPER.setSerializationInclusion(JsonInclude.Include.NON_NULL);
    DEFAULT_MAPPER.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
  }

  public String serialize() {
    try {
      return DEFAULT_MAPPER.writeValueAsString(this);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Deserialize a json string into a PolarisStorageConfiguration object
   *
   * @param diagnostics the diagnostics instance
   * @param jsonStr a json string
   * @return the PolarisStorageConfiguration object
   */
  public static PolarisStorageConfigurationInfo deserialize(
      @NotNull PolarisDiagnostics diagnostics, final @NotNull String jsonStr) {
    try {
      return DEFAULT_MAPPER.readValue(jsonStr, PolarisStorageConfigurationInfo.class);
    } catch (JsonProcessingException exception) {
      diagnostics.fail(
          "fail_to_deserialize_storage_configuration", exception, "jsonStr={}", jsonStr);
    }
    return null;
  }

  public static Optional<PolarisStorageConfigurationInfo> forEntityPath(
      PolarisDiagnostics diagnostics, List<PolarisEntity> entityPath) {
    return findStorageInfoFromHierarchy(entityPath)
        .map(
            storageInfo ->
                deserialize(
                    diagnostics,
                    storageInfo
                        .getInternalPropertiesAsMap()
                        .get(PolarisEntityConstants.getStorageConfigInfoPropertyName())))
        .map(
            configInfo -> {
              List<PolarisEntity> entityPathReversed = new ArrayList<>(entityPath);
              Collections.reverse(entityPathReversed);

              String baseLocation =
                  entityPathReversed.stream()
                      .flatMap(
                          e ->
                              Optional.ofNullable(
                                  e.getPropertiesAsMap()
                                      .get(PolarisEntityConstants.ENTITY_BASE_LOCATION))
                                  .stream())
                      .findFirst()
                      .orElse(null);
              CatalogEntity catalog = CatalogEntity.of(entityPath.get(0));
              boolean allowEscape =
                  CallContext.getCurrentContext()
                      .getPolarisCallContext()
                      .getConfigurationStore()
                      .getConfiguration(
                          CallContext.getCurrentContext().getPolarisCallContext(),
                          catalog,
                          PolarisConfiguration.ALLOW_UNSTRUCTURED_TABLE_LOCATION);
              if (!allowEscape
                  && catalog.getCatalogType() != Catalog.TypeEnum.EXTERNAL
                  && baseLocation != null) {
                LOGGER.debug(
                    "Not allowing unstructured table location for entity: {}",
                    entityPathReversed.get(0).getName());
                return new StorageConfigurationOverride(configInfo, List.of(baseLocation));
              } else {
                LOGGER.debug(
                    "Allowing unstructured table location for entity: {}",
                    entityPathReversed.get(0).getName());

                List<String> locs =
                    userSpecifiedWriteLocations(entityPathReversed.get(0).getPropertiesAsMap());
                return new StorageConfigurationOverride(
                    configInfo,
                    ImmutableList.<String>builder()
                        .addAll(configInfo.getAllowedLocations())
                        .addAll(locs)
                        .build());
              }
            });
  }

  private static List<String> userSpecifiedWriteLocations(Map<String, String> properties) {
    return Optional.ofNullable(properties)
        .map(
            p ->
                Stream.of(
                        p.get(TableLikeEntity.USER_SPECIFIED_WRITE_DATA_LOCATION_KEY),
                        p.get(TableLikeEntity.USER_SPECIFIED_WRITE_METADATA_LOCATION_KEY))
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList()))
        .orElse(List.of());
  }

  private static @NotNull Optional<PolarisEntity> findStorageInfoFromHierarchy(
      List<PolarisEntity> entityPath) {
    for (int i = entityPath.size() - 1; i >= 0; i--) {
      PolarisEntity e = entityPath.get(i);
      if (e.getInternalPropertiesAsMap()
          .containsKey(PolarisEntityConstants.getStorageConfigInfoPropertyName())) {
        return Optional.of(e);
      }
    }
    return Optional.empty();
  }

  /** Subclasses must provide the Iceberg FileIO impl associated with their type in this method. */
  public abstract String getFileIoImplClassName();

  /** Validate if the provided allowed locations are valid for the storage type */
  protected void validatePrefixForStorageType(String loc) {
    if (storageType.prefixes.stream().noneMatch(p -> loc.toLowerCase(Locale.ROOT).startsWith(p))) {
      throw new IllegalArgumentException(
          String.format(
              "Location prefix not allowed: '%s', expected prefixes: '%s'",
              loc, String.join(",", storageType.prefixes)));
    }
  }

  /** Validate the number of allowed locations not exceeding the max value. */
  public void validateMaxAllowedLocations(int maxAllowedLocations) {
    if (allowedLocations.size() > maxAllowedLocations) {
      throw new IllegalArgumentException(
          "Number of allowed locations exceeds " + maxAllowedLocations);
    }
  }

  /** Polaris' storage type, each has a fixed prefix for its location */
  public enum StorageType {
    S3("s3://"),
    S3_COMPATIBLE("s3://"),
    AZURE(List.of("abfs://", "wasb://", "abfss://", "wasbs://")),
    GCS("gs://"),
    FILE("file://"),
    ;

    private final List<String> prefixes;

    StorageType(String prefix) {
      this.prefixes = List.of(prefix);
    }

    StorageType(List<String> prefixes) {
      this.prefixes = prefixes;
    }

    public List<String> getPrefixes() {
      return prefixes;
    }
  }

  /** Enum property for describe storage integration for config purpose. */
  public enum DescribeProperty {
    STORAGE_PROVIDER,
    STORAGE_ALLOWED_LOCATIONS,
    STORAGE_AWS_ROLE_ARN,
    STORAGE_AWS_IAM_USER_ARN,
    STORAGE_AWS_EXTERNAL_ID,
    STORAGE_GCP_SERVICE_ACCOUNT,
    AZURE_TENANT_ID,
    AZURE_CONSENT_URL,
    AZURE_MULTI_TENANT_APP_NAME,
  }
}
