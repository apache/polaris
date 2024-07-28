/*
 * Copyright (c) 2024 Snowflake Computing Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.polaris.core.storage;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.polaris.core.PolarisDiagnostics;
import io.polaris.core.storage.aws.AwsStorageConfigurationInfo;
import io.polaris.core.storage.azure.AzureStorageConfigurationInfo;
import io.polaris.core.storage.gcp.GcpStorageConfigurationInfo;
import java.util.List;
import org.jetbrains.annotations.NotNull;

/**
 * The polaris storage configuration information, is part of a polaris entity's internal property,
 * that holds necessary information including
 *
 * <pre>
 * 1. locations that allows polaris to get access to
 * 2. cloud identity info that a service principle can request access token to the locations
 * </pre</>
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME)
@JsonSubTypes({
  @JsonSubTypes.Type(value = AwsStorageConfigurationInfo.class),
  @JsonSubTypes.Type(value = AzureStorageConfigurationInfo.class),
  @JsonSubTypes.Type(value = GcpStorageConfigurationInfo.class),
  @JsonSubTypes.Type(value = FileStorageConfigurationInfo.class),
})
public abstract class PolarisStorageConfigurationInfo {

  // a list of allowed locations
  public List<String> allowedLocations;

  // storage type
  public StorageType storageType;

  public PolarisStorageConfigurationInfo(
      @JsonProperty(value = "storageType", required = true) @NotNull StorageType storageType,
      @JsonProperty(value = "allowedLocations", required = true) @NotNull
          List<String> allowedLocations) {
    this.allowedLocations = allowedLocations;
    this.storageType = storageType;
    this.validatePrefixForStorageType();
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

  /** Subclasses must provide the Iceberg FileIO impl associated with their type in this method. */
  public abstract String getFileIoImplClassName();

  /** Validate if the provided allowed locations are valid for the storage type */
  public void validatePrefixForStorageType() {
    this.allowedLocations.forEach(
        loc -> {
          if (!loc.toLowerCase().startsWith(storageType.prefix)) {
            throw new IllegalArgumentException(
                String.format(
                    "Location prefix not allowed: '%s', expected prefix: '%s'",
                    loc, storageType.prefix));
          }
        });
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
