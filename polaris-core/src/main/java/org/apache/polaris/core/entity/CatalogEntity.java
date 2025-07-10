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
package org.apache.polaris.core.entity;

import static org.apache.polaris.core.admin.model.StorageConfigInfo.StorageTypeEnum.AZURE;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.iceberg.exceptions.BadRequestException;
import org.apache.polaris.core.PolarisDefaultDiagServiceImpl;
import org.apache.polaris.core.admin.model.AwsStorageConfigInfo;
import org.apache.polaris.core.admin.model.AzureStorageConfigInfo;
import org.apache.polaris.core.admin.model.Catalog;
import org.apache.polaris.core.admin.model.CatalogProperties;
import org.apache.polaris.core.admin.model.ConnectionConfigInfo;
import org.apache.polaris.core.admin.model.ExternalCatalog;
import org.apache.polaris.core.admin.model.FileStorageConfigInfo;
import org.apache.polaris.core.admin.model.GcpStorageConfigInfo;
import org.apache.polaris.core.admin.model.PolarisCatalog;
import org.apache.polaris.core.admin.model.StorageConfigInfo;
import org.apache.polaris.core.config.BehaviorChangeConfiguration;
import org.apache.polaris.core.connection.ConnectionConfigInfoDpo;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.secrets.UserSecretReference;
import org.apache.polaris.core.storage.FileStorageConfigurationInfo;
import org.apache.polaris.core.storage.PolarisStorageConfigurationInfo;
import org.apache.polaris.core.storage.aws.AwsStorageConfigurationInfo;
import org.apache.polaris.core.storage.azure.AzureStorageConfigurationInfo;
import org.apache.polaris.core.storage.gcp.GcpStorageConfigurationInfo;

/**
 * Catalog specific subclass of the {@link PolarisEntity} that handles conversion from the {@link
 * Catalog} model to the persistent entity model.
 */
public class CatalogEntity extends PolarisEntity implements LocationBasedEntity {
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

  public CatalogEntity(PolarisBaseEntity sourceEntity) {
    super(sourceEntity);
  }

  public static CatalogEntity of(PolarisBaseEntity sourceEntity) {
    if (sourceEntity != null) {
      return new CatalogEntity(sourceEntity);
    }
    return null;
  }

  public static CatalogEntity fromCatalog(CallContext callContext, Catalog catalog) {
    Builder builder =
        new Builder()
            .setName(catalog.getName())
            .setProperties(catalog.getProperties().toMap())
            .setCatalogType(catalog.getType().name());
    Map<String, String> internalProperties = new HashMap<>();
    internalProperties.put(CATALOG_TYPE_PROPERTY, catalog.getType().name());
    builder.setInternalProperties(internalProperties);
    builder.setStorageConfigurationInfo(
        callContext, catalog.getStorageConfigInfo(), getBaseLocation(catalog));
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
    return catalogType == Catalog.TypeEnum.EXTERNAL
        ? ExternalCatalog.builder()
            .setType(Catalog.TypeEnum.EXTERNAL)
            .setName(getName())
            .setProperties(catalogProps)
            .setCreateTimestamp(getCreateTimestamp())
            .setLastUpdateTimestamp(getLastUpdateTimestamp())
            .setEntityVersion(getEntityVersion())
            .setStorageConfigInfo(getStorageInfo(internalProperties))
            .setConnectionConfigInfo(getConnectionInfo(internalProperties))
            .build()
        : PolarisCatalog.builder()
            .setType(Catalog.TypeEnum.INTERNAL)
            .setName(getName())
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
      if (configInfo instanceof AwsStorageConfigurationInfo) {
        AwsStorageConfigurationInfo awsConfig = (AwsStorageConfigurationInfo) configInfo;
        return AwsStorageConfigInfo.builder()
            .setRoleArn(awsConfig.getRoleARN())
            .setExternalId(awsConfig.getExternalId())
            .setUserArn(awsConfig.getUserARN())
            .setStorageType(StorageConfigInfo.StorageTypeEnum.S3)
            .setAllowedLocations(awsConfig.getAllowedLocations())
            .setRegion(awsConfig.getRegion())
            .setEndpoint(awsConfig.getEndpoint())
            .setStsEndpoint(awsConfig.getStsEndpoint())
            .setPathStyleAccess(awsConfig.getPathStyleAccess())
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

  private ConnectionConfigInfo getConnectionInfo(Map<String, String> internalProperties) {
    if (internalProperties.containsKey(
        PolarisEntityConstants.getConnectionConfigInfoPropertyName())) {
      ConnectionConfigInfoDpo configInfo = getConnectionConfigInfoDpo();
      return configInfo.asConnectionConfigInfoModel();
    }
    return null;
  }

  @Override
  public String getBaseLocation() {
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

  public boolean isPassthroughFacade() {
    return getInternalPropertiesAsMap()
        .containsKey(PolarisEntityConstants.getConnectionConfigInfoPropertyName());
  }

  public ConnectionConfigInfoDpo getConnectionConfigInfoDpo() {
    String configStr =
        getInternalPropertiesAsMap()
            .get(PolarisEntityConstants.getConnectionConfigInfoPropertyName());
    if (configStr != null) {
      return ConnectionConfigInfoDpo.deserialize(new PolarisDefaultDiagServiceImpl(), configStr);
    }
    return null;
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
        CallContext callContext, StorageConfigInfo storageConfigModel, String defaultBaseLocation) {
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
        validateMaxAllowedLocations(callContext, allowedLocations);
        switch (storageConfigModel.getStorageType()) {
          case S3:
            AwsStorageConfigInfo awsConfigModel = (AwsStorageConfigInfo) storageConfigModel;
            AwsStorageConfigurationInfo awsConfig =
                new AwsStorageConfigurationInfo(
                    PolarisStorageConfigurationInfo.StorageType.S3,
                    new ArrayList<>(allowedLocations),
                    awsConfigModel.getRoleArn(),
                    awsConfigModel.getExternalId(),
                    awsConfigModel.getRegion(),
                    awsConfigModel.getEndpoint(),
                    awsConfigModel.getStsEndpoint(),
                    awsConfigModel.getPathStyleAccess());
            awsConfig.validateArn(awsConfigModel.getRoleArn());
            config = awsConfig;
            break;
          case AZURE:
            AzureStorageConfigInfo azureConfigModel = (AzureStorageConfigInfo) storageConfigModel;
            AzureStorageConfigurationInfo azureConfigInfo =
                new AzureStorageConfigurationInfo(
                    new ArrayList<>(allowedLocations), azureConfigModel.getTenantId());
            azureConfigInfo.setMultiTenantAppName(azureConfigModel.getMultiTenantAppName());
            azureConfigInfo.setConsentUrl(azureConfigModel.getConsentUrl());
            config = azureConfigInfo;
            break;
          case GCS:
            GcpStorageConfigurationInfo gcpConfig =
                new GcpStorageConfigurationInfo(new ArrayList<>(allowedLocations));
            gcpConfig.setGcpServiceAccount(
                ((GcpStorageConfigInfo) storageConfigModel).getGcsServiceAccount());
            config = gcpConfig;
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

    /** Validate the number of allowed locations not exceeding the max value. */
    private void validateMaxAllowedLocations(
        CallContext callContext, Collection<String> allowedLocations) {
      int maxAllowedLocations =
          callContext
              .getRealmConfig()
              .getConfig(BehaviorChangeConfiguration.STORAGE_CONFIGURATION_MAX_LOCATIONS);
      if (maxAllowedLocations != -1 && allowedLocations.size() > maxAllowedLocations) {
        throw new IllegalArgumentException(
            String.format(
                "Number of configured locations (%s) exceeds the limit of %s",
                allowedLocations.size(), maxAllowedLocations));
      }
    }

    public Builder setConnectionConfigInfoDpoWithSecrets(
        ConnectionConfigInfo connectionConfigurationModel,
        Map<String, UserSecretReference> secretReferences) {
      if (connectionConfigurationModel != null) {
        ConnectionConfigInfoDpo config =
            ConnectionConfigInfoDpo.fromConnectionConfigInfoModelWithSecrets(
                connectionConfigurationModel, secretReferences);
        internalProperties.put(
            PolarisEntityConstants.getConnectionConfigInfoPropertyName(), config.serialize());
      }
      return this;
    }

    @Override
    public CatalogEntity build() {
      return new CatalogEntity(buildBase());
    }
  }

  protected static @Nonnull String getBaseLocation(Catalog catalog) {
    return catalog.getProperties().getDefaultBaseLocation();
  }
}
