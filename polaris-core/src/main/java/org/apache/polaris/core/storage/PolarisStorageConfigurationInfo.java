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
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
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
import org.apache.polaris.core.storage.aws.ImmutableAwsStorageConfigurationInfo;
import org.apache.polaris.core.storage.azure.ImmutableAzureStorageConfigurationInfo;
import org.apache.polaris.core.storage.gcp.ImmutableGcpStorageConfigurationInfo;
import org.immutables.value.Value.Check;
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
  @JsonSubTypes.Type(value = ImmutableAwsStorageConfigurationInfo.class),
  @JsonSubTypes.Type(value = ImmutableAzureStorageConfigurationInfo.class),
  @JsonSubTypes.Type(value = ImmutableGcpStorageConfigurationInfo.class),
  @JsonSubTypes.Type(value = ImmutableFileStorageConfigurationInfo.class),
})
public abstract class PolarisStorageConfigurationInfo {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(PolarisStorageConfigurationInfo.class);

  public abstract List<String> getAllowedLocations();

  public abstract StorageType getStorageType();

  private static final ObjectMapper DEFAULT_MAPPER;

  static {
    DEFAULT_MAPPER = new ObjectMapper();
    DEFAULT_MAPPER.registerModule(new GuavaModule());
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
                return StorageConfigurationOverride.of(configInfo, List.of(baseLocation));
              } else {
                LOGGER.debug(
                    "Allowing unstructured table location for entity: {}",
                    entityPathReversed.get(0).getName());

                List<String> locs =
                    userSpecifiedWriteLocations(entityPathReversed.get(0).getPropertiesAsMap());
                return StorageConfigurationOverride.of(
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

  @Check
  protected void validate() {
    getAllowedLocations().forEach(this::validatePrefixForStorageType);
  }

  /** Validate if the provided allowed locations are valid for the storage type */
  protected void validatePrefixForStorageType(String loc) {
    if (!loc.toLowerCase(Locale.ROOT).startsWith(getStorageType().prefix)) {
      throw new IllegalArgumentException(
          String.format(
              "Location prefix not allowed: '%s', expected prefix: '%s'",
              loc, getStorageType().prefix));
    }
  }

  /** Validate the number of allowed locations not exceeding the max value. */
  protected void validateMaxAllowedLocations(int maxAllowedLocations) {
    if (getAllowedLocations().size() > maxAllowedLocations) {
      throw new IllegalArgumentException(
          "Number of allowed locations exceeds " + maxAllowedLocations);
    }
  }

  /** Polaris' storage type, each has a fixed prefix for its location */
  public enum StorageType {
    S3("s3://"),
    AZURE("abfs"), // abfs or abfss
    GCS("gs://"),
    FILE("file://"),
    ;

    final String prefix;

    StorageType(String prefix) {
      this.prefix = prefix;
    }

    public String getPrefix() {
      return prefix;
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
