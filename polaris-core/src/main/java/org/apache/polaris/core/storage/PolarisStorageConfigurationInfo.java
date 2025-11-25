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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import org.apache.polaris.core.admin.model.Catalog;
import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.entity.CatalogEntity;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PolarisEntityConstants;
import org.apache.polaris.core.storage.aws.AwsStorageConfigurationInfo;
import org.apache.polaris.core.storage.azure.AzureStorageConfigurationInfo;
import org.apache.polaris.core.storage.gcp.GcpStorageConfigurationInfo;
import org.immutables.value.Value;
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
  @JsonSubTypes.Type(value = AzureStorageConfigurationInfo.class),
  @JsonSubTypes.Type(value = GcpStorageConfigurationInfo.class),
  @JsonSubTypes.Type(value = FileStorageConfigurationInfo.class),
})
@JsonIgnoreProperties(ignoreUnknown = true)
public abstract class PolarisStorageConfigurationInfo {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(PolarisStorageConfigurationInfo.class);

  @Value.Check
  protected void check() {
    if (validatePrefix()) {
      getAllowedLocations().forEach(this::validatePrefixForStorageType);
    }
  }

  @JsonIgnore
  @Value.Auxiliary
  public boolean validatePrefix() {
    return true;
  }

  public abstract List<String> getAllowedLocations();

  public abstract StorageType getStorageType();

  private static final ObjectMapper DEFAULT_MAPPER;

  static {
    DEFAULT_MAPPER = new ObjectMapper();
    DEFAULT_MAPPER.setDefaultPropertyInclusion(JsonInclude.Include.NON_NULL);
    DEFAULT_MAPPER.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
  }

  public String serialize() {
    try {
      return DEFAULT_MAPPER.writeValueAsString(this);
    } catch (JsonProcessingException e) {
      throw new RuntimeException("serialize failed: " + e.getMessage(), e);
    }
  }

  /**
   * Deserialize a json string into a PolarisStorageConfiguration object
   *
   * @param jsonStr a json string
   * @return the PolarisStorageConfiguration object
   */
  public static PolarisStorageConfigurationInfo deserialize(final @Nonnull String jsonStr) {
    try {
      return DEFAULT_MAPPER.readValue(jsonStr, PolarisStorageConfigurationInfo.class);
    } catch (JsonProcessingException e) {
      throw new RuntimeException("deserialize failed: " + e.getMessage(), e);
    }
  }

  public static Optional<LocationRestrictions> getLocationRestrictionsForEntityPath(
      RealmConfig realmConfig, List<PolarisEntity> entityPath) {
    return findStorageInfoFromHierarchy(entityPath)
        .map(
            storageInfo ->
                deserialize(
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
                  realmConfig.getConfig(
                      FeatureConfiguration.ALLOW_UNSTRUCTURED_TABLE_LOCATION, catalog);
              if (!allowEscape
                  && catalog.getCatalogType() != Catalog.TypeEnum.EXTERNAL
                  && baseLocation != null) {
                LOGGER.debug(
                    "Not allowing unstructured table location for entity: {}",
                    entityPathReversed.get(0).getName());
                return new LocationRestrictions(configInfo, baseLocation);
              } else {
                LOGGER.debug(
                    "Allowing unstructured table location for entity: {}",
                    entityPathReversed.get(0).getName());

                return new LocationRestrictions(configInfo);
              }
            });
  }

  private static @Nonnull Optional<PolarisEntity> findStorageInfoFromHierarchy(
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
    if (getStorageType().prefixes.stream()
        .noneMatch(p -> loc.toLowerCase(Locale.ROOT).startsWith(p))) {
      throw new IllegalArgumentException(
          String.format(
              "Location prefix not allowed: '%s', expected prefixes: '%s'",
              loc, String.join(",", getStorageType().prefixes)));
    }
  }

  /** Polaris' storage type, each has a fixed prefix for its location */
  public enum StorageType {
    S3(List.of("s3://", "s3a://")),
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
}
