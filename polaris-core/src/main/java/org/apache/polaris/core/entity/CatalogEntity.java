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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import org.apache.iceberg.exceptions.BadRequestException;
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
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.connection.ConnectionConfigInfoDpo;
import org.apache.polaris.core.identity.dpo.ServiceIdentityInfoDpo;
import org.apache.polaris.core.identity.provider.ServiceIdentityProvider;
import org.apache.polaris.core.secrets.SecretReference;
import org.apache.polaris.core.storage.FileStorageConfigurationInfo;
import org.apache.polaris.core.storage.PolarisStorageConfigurationInfo;
import org.apache.polaris.core.storage.StorageLocation;
import org.apache.polaris.core.storage.aws.AwsStorageConfigurationInfo;
import org.apache.polaris.core.storage.azure.AzureStorageConfigurationInfo;
import org.apache.polaris.core.storage.gcp.GcpStorageConfigurationInfo;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/**
 * Catalog specific subclass of the {@link PolarisEntity} that handles conversion from the {@link
 * Catalog} model to the persistent entity model.
 */
public class CatalogEntity extends PolarisEntity implements LocationBasedEntity {
  public static final String CATALOG_TYPE_PROPERTY = "catalogType";

  // Specifies the object-store base location used for all Table file locations under the
  // catalog, stored in the "properties" map.
  public static final String DEFAULT_BASE_LOCATION_KEY = "default-base-location";

  public CatalogEntity(PolarisBaseEntity sourceEntity) {
    super(sourceEntity);
    Preconditions.checkState(
        getType() == PolarisEntityType.CATALOG, "Invalid entity type: %s", getType());
    Preconditions.checkState(
        getSubType() == PolarisEntitySubType.NULL_SUBTYPE,
        "Invalid entity sub type: %s",
        getSubType());
  }

  public static @Nullable CatalogEntity of(@Nullable PolarisBaseEntity sourceEntity) {
    if (sourceEntity != null) {
      return new CatalogEntity(sourceEntity);
    }
    return null;
  }

  public static CatalogEntity fromCatalog(RealmConfig realmConfig, Catalog catalog) {
    Builder builder =
        new Builder()
            .setName(catalog.getName())
            .setProperties(catalog.getProperties().toMap())
            .setCatalogType(catalog.getType().name());
    Map<String, String> internalProperties = new HashMap<>();
    internalProperties.put(CATALOG_TYPE_PROPERTY, catalog.getType().name());
    builder.setInternalProperties(internalProperties);
    builder.setStorageConfigurationInfo(
        realmConfig, catalog.getStorageConfigInfo(), getBaseLocation(catalog));
    return builder.build();
  }

  public Catalog asCatalog() {
    return this.asCatalog(null);
  }

  public Catalog asCatalog(ServiceIdentityProvider serviceIdentityProvider) {
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

    // Right now, only external catalog may use ServiceIdentityProvider to resolve identity
    Preconditions.checkState(
        catalogType != Catalog.TypeEnum.EXTERNAL || serviceIdentityProvider != null,
        "%s catalog needs ServiceIdentityProvider to resolve service identities",
        Catalog.TypeEnum.EXTERNAL);
    return catalogType == Catalog.TypeEnum.EXTERNAL
        ? ExternalCatalog.builder()
            .setType(Catalog.TypeEnum.EXTERNAL)
            .setName(getName())
            .setProperties(catalogProps)
            .setCreateTimestamp(getCreateTimestamp())
            .setLastUpdateTimestamp(getLastUpdateTimestamp())
            .setEntityVersion(getEntityVersion())
            .setStorageConfigInfo(getStorageInfo(internalProperties))
            .setConnectionConfigInfo(getConnectionInfo(internalProperties, serviceIdentityProvider))
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
      if (configInfo instanceof AwsStorageConfigurationInfo awsConfig) {
        return AwsStorageConfigInfo.builder()
            .setRoleArn(awsConfig.getRoleARN())
            .setExternalId(awsConfig.getExternalId())
            .setUserArn(awsConfig.getUserARN())
            .setCurrentKmsKey(awsConfig.getCurrentKmsKey())
            .setAllowedKmsKeys(awsConfig.getAllowedKmsKeys())
            .setStorageType(StorageConfigInfo.StorageTypeEnum.S3)
            .setAllowedLocations(awsConfig.getAllowedLocations())
            .setStorageName(awsConfig.getStorageName())
            .setRegion(awsConfig.getRegion())
            .setEndpoint(awsConfig.getEndpoint())
            .setStsEndpoint(awsConfig.getStsEndpoint())
            .setPathStyleAccess(awsConfig.getPathStyleAccess())
            .setStsUnavailable(awsConfig.getStsUnavailable())
            .setEndpointInternal(awsConfig.getEndpointInternal())
            .setKmsUnavailable(awsConfig.getKmsUnavailable())
            .build();
      }
      if (configInfo instanceof AzureStorageConfigurationInfo azureConfig) {
        return AzureStorageConfigInfo.builder()
            .setTenantId(azureConfig.getTenantId())
            .setMultiTenantAppName(azureConfig.getMultiTenantAppName())
            .setConsentUrl(azureConfig.getConsentUrl())
            .setStorageType(AZURE)
            .setAllowedLocations(azureConfig.getAllowedLocations())
            .setStorageName(azureConfig.getStorageName())
            .setHierarchical(azureConfig.isHierarchical())
            .build();
      }
      if (configInfo instanceof GcpStorageConfigurationInfo gcpConfigModel) {
        return GcpStorageConfigInfo.builder()
            .setGcsServiceAccount(gcpConfigModel.getGcpServiceAccount())
            .setStorageType(StorageConfigInfo.StorageTypeEnum.GCS)
            .setAllowedLocations(gcpConfigModel.getAllowedLocations())
            .setStorageName(gcpConfigModel.getStorageName())
            .build();
      }
      if (configInfo instanceof FileStorageConfigurationInfo fileConfigModel) {
        return FileStorageConfigInfo.builder()
            .setStorageType(StorageConfigInfo.StorageTypeEnum.FILE)
            .setAllowedLocations(fileConfigModel.getAllowedLocations())
            .setStorageName(fileConfigModel.getStorageName())
            .build();
      }
      return null;
    }
    return null;
  }

  private ConnectionConfigInfo getConnectionInfo(
      Map<String, String> internalProperties, ServiceIdentityProvider serviceIdentityProvider) {
    if (internalProperties.containsKey(
        PolarisEntityConstants.getConnectionConfigInfoPropertyName())) {
      ConnectionConfigInfoDpo configInfo = getConnectionConfigInfoDpo();
      return configInfo.asConnectionConfigInfoModel(serviceIdentityProvider);
    }
    return null;
  }

  @Override
  public String getBaseLocation() {
    return getPropertiesAsMap().get(DEFAULT_BASE_LOCATION_KEY);
  }

  public @Nullable PolarisStorageConfigurationInfo getStorageConfigurationInfo() {
    String configStr =
        getInternalPropertiesAsMap().get(PolarisEntityConstants.getStorageConfigInfoPropertyName());
    if (configStr != null) {
      return PolarisStorageConfigurationInfo.deserialize(configStr);
    }
    return null;
  }

  public Catalog.TypeEnum getCatalogType() {
    return Optional.ofNullable(getInternalPropertiesAsMap().get(CATALOG_TYPE_PROPERTY))
        .map(Catalog.TypeEnum::valueOf)
        .orElse(null);
  }

  public boolean isExternal() {
    return getCatalogType() == Catalog.TypeEnum.EXTERNAL;
  }

  public boolean isPassthroughFacade() {
    return getInternalPropertiesAsMap()
        .containsKey(PolarisEntityConstants.getConnectionConfigInfoPropertyName());
  }

  public boolean isStaticFacade() {
    return isExternal() && !isPassthroughFacade();
  }

  public ConnectionConfigInfoDpo getConnectionConfigInfoDpo() {
    String configStr =
        getInternalPropertiesAsMap()
            .get(PolarisEntityConstants.getConnectionConfigInfoPropertyName());
    if (configStr != null) {
      return ConnectionConfigInfoDpo.deserialize(configStr);
    }
    return null;
  }

  /**
   * Validates {@code defaultBaseLocation} against the given allowed-locations list.
   *
   * <p>Rejects null/empty allowed-locations to match the runtime semantic in {@code
   * InMemoryStorageIntegration} (an empty allowed-list means "no location is allowed", not "no
   * constraint"). Then verifies {@code defaultBaseLocation} is a subpath of at least one entry.
   *
   * @throws BadRequestException if {@code allowedLocations} is null/empty, or if {@code
   *     defaultBaseLocation} is not within any entry of {@code allowedLocations}
   */
  @VisibleForTesting
  public static void validateBaseLocationAgainstAllowedList(
      List<String> allowedLocations, String defaultBaseLocation) {
    if (allowedLocations == null || allowedLocations.isEmpty()) {
      throw new BadRequestException(
          "Cannot set default-base-location '%s': storage configuration has no allowed-locations"
              + " (allowed-locations list is %s)",
          defaultBaseLocation, allowedLocations == null ? "null" : "empty");
    }

    StorageLocation baseLocation = StorageLocation.of(defaultBaseLocation);
    boolean isAllowed =
        allowedLocations.stream()
            .filter(Objects::nonNull)
            .map(StorageLocation::of)
            .anyMatch(baseLocation::isChildOf);

    if (!isAllowed) {
      throw new BadRequestException(
          "default-base-location '%s' is not within any of the allowed locations %s",
          defaultBaseLocation, allowedLocations);
    }
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
      // Note that this member lives in the main 'properties' map rather than internalProperties.
      properties.put(DEFAULT_BASE_LOCATION_KEY, defaultBaseLocation);
      return this;
    }

    public Builder validateDefaultBaseLocation() {
      String defaultBaseLocation = properties.get(DEFAULT_BASE_LOCATION_KEY);
      if (defaultBaseLocation == null) {
        return this;
      }
      String configStr =
          internalProperties.get(PolarisEntityConstants.getStorageConfigInfoPropertyName());
      if (configStr == null) {
        return this;
      }
      PolarisStorageConfigurationInfo storageConfig =
          PolarisStorageConfigurationInfo.deserialize(configStr);
      if (storageConfig == null) {
        return this;
      }
      List<String> allowedLocations = storageConfig.getAllowedLocations();
      if (allowedLocations != null && !allowedLocations.isEmpty()) {
        validateBaseLocationAgainstAllowedList(allowedLocations, defaultBaseLocation);
      }
      return this;
    }

    public Builder setStorageConfigurationInfo(
        RealmConfig realmConfig, StorageConfigInfo storageConfigModel, String defaultBaseLocation) {
      if (storageConfigModel != null) {
        if (defaultBaseLocation == null) {
          throw new BadRequestException("Must specify default base location");
        }
        // Asymmetric semantics for the simple-create case vs. explicit input:
        //   - If the caller supplied no allowed-locations, default to the catalog's
        //     default-base-location. This is the convenience for naive create requests.
        //     (Update-time callers in PolarisAdminService enforce strictness before reaching
        //     this method, so the auto-populate only kicks in at create time.)
        //   - If the caller supplied an explicit allowed-locations list, validate that the
        //     default-base-location is a subpath of at least one entry and store the list
        //     as-is. No silent additions to the user-supplied list.
        List<String> userAllowedLocations = storageConfigModel.getAllowedLocations();
        Set<String> allowedLocations;
        if (userAllowedLocations == null || userAllowedLocations.isEmpty()) {
          allowedLocations = new HashSet<>();
          allowedLocations.add(defaultBaseLocation);
        } else {
          validateBaseLocationAgainstAllowedList(userAllowedLocations, defaultBaseLocation);
          allowedLocations = new HashSet<>(userAllowedLocations);
        }
        PolarisStorageConfigurationInfo config;
        validateMaxAllowedLocations(realmConfig, allowedLocations);
        switch (storageConfigModel.getStorageType()) {
          case S3:
            AwsStorageConfigInfo awsConfigModel = (AwsStorageConfigInfo) storageConfigModel;
            AwsStorageConfigurationInfo awsConfig =
                AwsStorageConfigurationInfo.builder()
                    .allowedLocations(allowedLocations)
                    .storageName(storageConfigModel.getStorageName())
                    .roleARN(awsConfigModel.getRoleArn())
                    .currentKmsKey(awsConfigModel.getCurrentKmsKey())
                    .allowedKmsKeys(awsConfigModel.getAllowedKmsKeys())
                    .externalId(awsConfigModel.getExternalId())
                    .region(awsConfigModel.getRegion())
                    .endpoint(awsConfigModel.getEndpoint())
                    .stsEndpoint(awsConfigModel.getStsEndpoint())
                    .pathStyleAccess(awsConfigModel.getPathStyleAccess())
                    .stsUnavailable(awsConfigModel.getStsUnavailable())
                    .endpointInternal(awsConfigModel.getEndpointInternal())
                    .kmsUnavailable(awsConfigModel.getKmsUnavailable())
                    .build();
            config = awsConfig;
            break;
          case AZURE:
            AzureStorageConfigInfo azureConfigModel = (AzureStorageConfigInfo) storageConfigModel;
            config =
                AzureStorageConfigurationInfo.builder()
                    .allowedLocations(allowedLocations)
                    .storageName(storageConfigModel.getStorageName())
                    .tenantId(azureConfigModel.getTenantId())
                    .multiTenantAppName(azureConfigModel.getMultiTenantAppName())
                    .consentUrl(azureConfigModel.getConsentUrl())
                    .hierarchical(azureConfigModel.getHierarchical())
                    .build();
            break;
          case GCS:
            config =
                GcpStorageConfigurationInfo.builder()
                    .allowedLocations(allowedLocations)
                    .storageName(storageConfigModel.getStorageName())
                    .gcpServiceAccount(
                        ((GcpStorageConfigInfo) storageConfigModel).getGcsServiceAccount())
                    .build();
            break;
          case FILE:
            config =
                FileStorageConfigurationInfo.builder()
                    .allowedLocations(allowedLocations)
                    .storageName(storageConfigModel.getStorageName())
                    .build();
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
        RealmConfig realmConfig, Collection<String> allowedLocations) {
      int maxAllowedLocations =
          realmConfig.getConfig(BehaviorChangeConfiguration.STORAGE_CONFIGURATION_MAX_LOCATIONS);
      if (maxAllowedLocations != -1 && allowedLocations.size() > maxAllowedLocations) {
        throw new IllegalArgumentException(
            String.format(
                "Number of configured locations (%s) exceeds the limit of %s",
                allowedLocations.size(), maxAllowedLocations));
      }
    }

    public Builder setConnectionConfigInfoDpoWithSecrets(
        ConnectionConfigInfo connectionConfigurationModel,
        Map<String, SecretReference> secretReferences,
        ServiceIdentityInfoDpo serviceIdentityInfoDpo) {
      if (connectionConfigurationModel != null) {
        ConnectionConfigInfoDpo config =
            ConnectionConfigInfoDpo.fromConnectionConfigInfoModelWithSecrets(
                    connectionConfigurationModel, secretReferences)
                .withServiceIdentity(serviceIdentityInfoDpo);
        internalProperties.put(
            PolarisEntityConstants.getConnectionConfigInfoPropertyName(), config.serialize());
      }
      return this;
    }

    public Builder setConnectionConfigInfoDpo(
        @NonNull ConnectionConfigInfoDpo connectionConfigInfoDpo) {
      internalProperties.put(
          PolarisEntityConstants.getConnectionConfigInfoPropertyName(),
          connectionConfigInfoDpo.serialize());
      return this;
    }

    @Override
    public CatalogEntity build() {
      validateDefaultBaseLocation();
      return new CatalogEntity(buildBase());
    }
  }

  protected static @NonNull String getBaseLocation(Catalog catalog) {
    return catalog.getProperties().getDefaultBaseLocation();
  }
}
