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
package io.polaris.core.entity;

import static io.polaris.core.admin.model.StorageConfigInfo.StorageTypeEnum.AZURE;

import io.polaris.core.PolarisDefaultDiagServiceImpl;
import io.polaris.core.admin.model.AwsStorageConfigInfo;
import io.polaris.core.admin.model.AzureStorageConfigInfo;
import io.polaris.core.admin.model.Catalog;
import io.polaris.core.admin.model.CatalogProperties;
import io.polaris.core.admin.model.ExternalCatalog;
import io.polaris.core.admin.model.FileStorageConfigInfo;
import io.polaris.core.admin.model.GcpStorageConfigInfo;
import io.polaris.core.admin.model.PolarisCatalog;
import io.polaris.core.admin.model.StorageConfigInfo;
import io.polaris.core.storage.FileStorageConfigurationInfo;
import io.polaris.core.storage.PolarisStorageConfigurationInfo;
import io.polaris.core.storage.aws.AwsStorageConfigurationInfo;
import io.polaris.core.storage.azure.AzureStorageConfigurationInfo;
import io.polaris.core.storage.gcp.GcpStorageConfigurationInfo;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.iceberg.exceptions.BadRequestException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Catalog specific subclass of the {@link PolarisEntity} that handles conversion from the {@link
 * Catalog} model to the persistent entity model.
 */
public class CatalogEntity extends PolarisEntity {
  private static final Logger LOG = LoggerFactory.getLogger(CatalogEntity.class);

  public static final long ROOT_CATALOG_ID = 0;
  public static final String CATALOG_TYPE_PROPERTY = "catalogType";

  // Specifies the object-store base location used for all Table file locations under the
  // catalog, stored in the "properties" map.
  public static final String DEFAULT_BASE_LOCATION_KEY = "default-base-location";

  // Specifies a prefix that will be replaced with the catalog's default-base-location whenever
  // it matches a specified new table or view location. For example, if the catalog base location
  // is "s3://my-bucket/base/location" and the prefix specified here is "file:/tmp" then any
  // new table attempting to specify a base location of "file:/tmp/ns1/ns2/table1" will be
  // translated into "s3://my-bucket/base/location/ns1/ns2/table1".
  public static final String REPLACE_NEW_LOCATION_PREFIX_WITH_CATALOG_DEFAULT_KEY =
      "replace-new-location-prefix-with-catalog-default";
  public static final String REMOTE_URL = "remoteUrl";

  public CatalogEntity(PolarisBaseEntity sourceEntity) {
    super(sourceEntity);
  }

  public static CatalogEntity of(PolarisBaseEntity sourceEntity) {
    if (sourceEntity != null) {
      return new CatalogEntity(sourceEntity);
    }
    return null;
  }

  public static CatalogEntity fromCatalog(Catalog catalog) {

    Builder builder =
        new Builder()
            .setName(catalog.getName())
            .setProperties(catalog.getProperties().toMap())
            .setCatalogType(catalog.getType().name());
    Map<String, String> internalProperties = new HashMap<>();
    if (catalog instanceof ExternalCatalog) {
      internalProperties.put(REMOTE_URL, ((ExternalCatalog) catalog).getRemoteUrl());
    }
    internalProperties.put(CATALOG_TYPE_PROPERTY, catalog.getType().name());
    builder.setInternalProperties(internalProperties);
    builder.setStorageConfigurationInfo(
        catalog.getStorageConfigInfo(), getDefaultBaseLocation(catalog));
    return builder.build();
  }

  public Catalog asCatalog() {
    Map<String, String> internalProperties = getInternalPropertiesAsMap();
    Catalog.TypeEnum catalogType =
        Optional.ofNullable(internalProperties.get(CATALOG_TYPE_PROPERTY))
            .map(Catalog.TypeEnum::valueOf)
            .orElseGet(() -> getName().equalsIgnoreCase("ROOT") ? Catalog.TypeEnum.INTERNAL : null);
    Map<String, String> propertiesMap = getPropertiesAsMap();
    CatalogProperties catalogProps =
        CatalogProperties.builder(propertiesMap.get(DEFAULT_BASE_LOCATION_KEY))
            .putAll(propertiesMap)
            .build();
    return catalogType == Catalog.TypeEnum.INTERNAL
        ? PolarisCatalog.builder()
            .setType(Catalog.TypeEnum.INTERNAL)
            .setName(getName())
            .setProperties(catalogProps)
            .setCreateTimestamp(getCreateTimestamp())
            .setLastUpdateTimestamp(getLastUpdateTimestamp())
            .setEntityVersion(getEntityVersion())
            .setStorageConfigInfo(getStorageInfo(internalProperties))
            .build()
        : ExternalCatalog.builder()
            .setType(Catalog.TypeEnum.EXTERNAL)
            .setName(getName())
            .setRemoteUrl(getInternalPropertiesAsMap().get(REMOTE_URL))
            .setProperties(catalogProps)
            .setCreateTimestamp(getCreateTimestamp())
            .setLastUpdateTimestamp(getLastUpdateTimestamp())
            .setEntityVersion(getEntityVersion())
            .setStorageConfigInfo(getStorageInfo(internalProperties))
            .build();
  }

  private StorageConfigInfo getStorageInfo(Map<String, String> internalProperties) {
    if (internalProperties.containsKey(PolarisEntityConstants.getStorageConfigInfoPropertyName())) {
      PolarisStorageConfigurationInfo configInfo = getStorageConfigurationInfo();
      PolarisStorageConfigurationInfo.StorageType storageType = configInfo.getStorageType();
      if (configInfo instanceof AwsStorageConfigurationInfo) {
        AwsStorageConfigurationInfo awsConfig = (AwsStorageConfigurationInfo) configInfo;
        return AwsStorageConfigInfo.builder()
            .setRoleArn(awsConfig.getRoleARN())
            .setExternalId(awsConfig.getExternalId())
            .setUserArn(awsConfig.getUserARN())
            .setStorageType(StorageConfigInfo.StorageTypeEnum.S3)
            .setAllowedLocations(awsConfig.getAllowedLocations())
            .build();
      }
      if (configInfo instanceof AzureStorageConfigurationInfo) {
        AzureStorageConfigurationInfo azureConfig = (AzureStorageConfigurationInfo) configInfo;
        return AzureStorageConfigInfo.builder()
            .setTenantId(azureConfig.getTenantId())
            .setMultiTenantAppName(azureConfig.getMultiTenantAppName())
            .setConsentUrl(azureConfig.getConsentUrl())
            .setStorageType(AZURE)
            .setAllowedLocations(azureConfig.getAllowedLocations())
            .setAuthType(azureConfig.getAuthType())
            .build();
      }
      if (configInfo instanceof GcpStorageConfigurationInfo) {
        GcpStorageConfigurationInfo gcpConfigModel = (GcpStorageConfigurationInfo) configInfo;
        return GcpStorageConfigInfo.builder()
            .setGcsServiceAccount(gcpConfigModel.getGcpServiceAccount())
            .setStorageType(StorageConfigInfo.StorageTypeEnum.GCS)
            .setAllowedLocations(gcpConfigModel.getAllowedLocations())
            .build();
      }
      if (configInfo instanceof FileStorageConfigurationInfo) {
        FileStorageConfigurationInfo fileConfigModel = (FileStorageConfigurationInfo) configInfo;
        return new FileStorageConfigInfo(
            StorageConfigInfo.StorageTypeEnum.FILE, fileConfigModel.getAllowedLocations());
      }
      return null;
    }
    return null;
  }

  public String getDefaultBaseLocation() {
    return getPropertiesAsMap().get(DEFAULT_BASE_LOCATION_KEY);
  }

  public String getReplaceNewLocationPrefixWithCatalogDefault() {
    return getPropertiesAsMap().get(REPLACE_NEW_LOCATION_PREFIX_WITH_CATALOG_DEFAULT_KEY);
  }

  public @Nullable PolarisStorageConfigurationInfo getStorageConfigurationInfo() {
    String configStr =
        getInternalPropertiesAsMap().get(PolarisEntityConstants.getStorageConfigInfoPropertyName());
    if (configStr != null) {
      return PolarisStorageConfigurationInfo.deserialize(
          new PolarisDefaultDiagServiceImpl(), configStr);
    }
    return null;
  }

  public Catalog.TypeEnum getCatalogType() {
    return Optional.ofNullable(getInternalPropertiesAsMap().get(CATALOG_TYPE_PROPERTY))
        .map(Catalog.TypeEnum::valueOf)
        .orElse(null);
  }

  public static class Builder extends PolarisEntity.BaseBuilder<CatalogEntity, Builder> {
    public Builder() {
      super();
      setType(PolarisEntityType.CATALOG);
      setCatalogId(PolarisEntityConstants.getNullId());
      setParentId(PolarisEntityConstants.getRootEntityId());
    }

    public Builder(CatalogEntity original) {
      super(original);
    }

    public Builder setCatalogType(String type) {
      internalProperties.put(CATALOG_TYPE_PROPERTY, type);
      return this;
    }

    public Builder setDefaultBaseLocation(String defaultBaseLocation) {
      // Note that this member lives in the main 'properties' map rather tha internalProperties.
      properties.put(DEFAULT_BASE_LOCATION_KEY, defaultBaseLocation);
      return this;
    }

    public Builder setReplaceNewLocationPrefixWithCatalogDefault(String value) {
      // Note that this member lives in the main 'properties' map rather tha internalProperties.
      properties.put(REPLACE_NEW_LOCATION_PREFIX_WITH_CATALOG_DEFAULT_KEY, value);
      return this;
    }

    public Builder setStorageConfigurationInfo(
        StorageConfigInfo storageConfigModel, String defaultBaseLocation) {
      if (storageConfigModel != null) {
        PolarisStorageConfigurationInfo config;
        Set<String> allowedLocations = new HashSet<>(storageConfigModel.getAllowedLocations());

        // TODO: Reconsider whether this should actually just be a check up-front or if we
        // actually want to silently add to the allowed locations. Maybe ideally we only
        // add to the allowedLocations if allowedLocations is empty for the simple case,
        // but if the caller provided allowedLocations explicitly, then we just verify that
        // the defaultBaseLocation is at least a subpath of one of the allowedLocations.
        if (defaultBaseLocation == null) {
          throw new BadRequestException("Must specify default base location");
        }
        allowedLocations.add(defaultBaseLocation);
        switch (storageConfigModel.getStorageType()) {
          case S3:
            AwsStorageConfigInfo awsConfigModel = (AwsStorageConfigInfo) storageConfigModel;
            config =
                new AwsStorageConfigurationInfo(
                    PolarisStorageConfigurationInfo.StorageType.S3,
                    new ArrayList<>(allowedLocations),
                    awsConfigModel.getRoleArn(),
                    awsConfigModel.getExternalId());
            ((AwsStorageConfigurationInfo) config).validateArn(awsConfigModel.getRoleArn());
            break;
          case AZURE:
            AzureStorageConfigInfo azureConfigModel = (AzureStorageConfigInfo) storageConfigModel;
            config =
                new AzureStorageConfigurationInfo(
                    new ArrayList<>(allowedLocations),
                    azureConfigModel.getTenantId(),
                    azureConfigModel.getAuthType());
            break;
          case GCS:
            config = new GcpStorageConfigurationInfo(new ArrayList<>(allowedLocations));
            break;
          case FILE:
            config = new FileStorageConfigurationInfo(new ArrayList<>(allowedLocations));
            break;
          default:
            throw new IllegalStateException(
                "Unsupported storage type: " + storageConfigModel.getStorageType());
        }
        internalProperties.put(
            PolarisEntityConstants.getStorageConfigInfoPropertyName(), config.serialize());
      }
      return this;
    }

    public CatalogEntity build() {
      return new CatalogEntity(buildBase());
    }
  }

  protected static @NotNull String getDefaultBaseLocation(Catalog catalog) {
    return catalog.getProperties().getDefaultBaseLocation();
  }
}
