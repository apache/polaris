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

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;
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
    builder.setStorageConfigurationInfo(realmConfig, catalog.getStorageConfigInfo());
    builder.setStorageConfigurationInfos(realmConfig, catalog.getStorageConfigInfos());
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
    List<StorageConfigInfo> namedStorageConfigInfos = getNamedStorageInfosForResponse();
    return catalogType == Catalog.TypeEnum.EXTERNAL
        ? ExternalCatalog.builder()
            .setType(Catalog.TypeEnum.EXTERNAL)
            .setName(getName())
            .setProperties(catalogProps)
            .setCreateTimestamp(getCreateTimestamp())
            .setLastUpdateTimestamp(getLastUpdateTimestamp())
            .setEntityVersion(getEntityVersion())
            .setStorageConfigInfo(getStorageInfo(internalProperties))
            .setStorageConfigInfos(namedStorageConfigInfos)
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
            .setStorageConfigInfos(namedStorageConfigInfos)
            .build();
  }

  private StorageConfigInfo getStorageInfo(Map<String, String> internalProperties) {
    if (internalProperties.containsKey(PolarisEntityConstants.getStorageConfigInfoPropertyName())) {
      return toStorageConfigInfoModel(getStorageConfigurationInfo());
    }
    return null;
  }

  // Null (not an empty list) when there are no named configs, so a catalog with none round-trips
  // to a response byte-identical to one from before this capability existed: no
  // `storageConfigInfos` key at all, since the generated model is annotated @JsonInclude(NON_NULL).

  private @Nullable List<StorageConfigInfo> getNamedStorageInfosForResponse() {
    Map<String, PolarisStorageConfigurationInfo> namedConfigs = getNamedStorageConfigurationInfos();
    if (namedConfigs.isEmpty()) {
      return null;
    }
    return namedConfigs.values().stream().map(this::toStorageConfigInfoModel).toList();
  }

  private StorageConfigInfo toStorageConfigInfoModel(PolarisStorageConfigurationInfo configInfo) {
    if (configInfo instanceof AwsStorageConfigurationInfo awsConfig) {
      return getAwsStorageConfigInfo(awsConfig);
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

  @SuppressWarnings("deprecation")
  private static AwsStorageConfigInfo getAwsStorageConfigInfo(
      AwsStorageConfigurationInfo awsConfig) {
    List<String> encryptionKeys = awsConfig.getEffectiveEncryptionKeys();
    return AwsStorageConfigInfo.builder()
        .setRoleArn(awsConfig.getRoleARN())
        .setExternalId(awsConfig.getExternalId())
        .setUserArn(awsConfig.getUserARN())
        .setCurrentKmsKey(awsConfig.getCurrentKmsKey())
        .setAllowedKmsKeys(encryptionKeys)
        .setEncryptionKeys(encryptionKeys)
        .setDecryptionKeys(awsConfig.getDecryptionKeys())
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

  public Map<String, PolarisStorageConfigurationInfo> getNamedStorageConfigurationInfos() {
    String configStr =
        getInternalPropertiesAsMap()
            .get(PolarisEntityConstants.getStorageConfigInfosPropertyName());
    if (configStr != null) {
      return PolarisStorageConfigurationInfo.deserializeMap(configStr);
    }
    return Map.of();
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
  private static void validateBaseLocationAgainstAllowedList(
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
    // The accepted set matches PR #4023's `StorageNameValidator`: trimmed, non-empty, and made up
    // only of alphanumerics, underscores, and hyphens, up to 128 characters.
    private static final Pattern STORAGE_NAME_PATTERN = Pattern.compile("^[a-zA-Z0-9_-]{1,128}$");

    private RealmConfig realmConfig;
    private StorageConfigInfo storageConfigModel;
    // Nullable and distinct from "empty": null means "not supplied, leave the persisted named-map
    // untouched"; empty means "supplied and empty, so remove every named entry" (design.md D6a).
    private List<StorageConfigInfo> namedStorageConfigModels;

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

    private void validateDefaultBaseLocation() {
      String defaultBaseLocation = properties.get(DEFAULT_BASE_LOCATION_KEY);
      if (defaultBaseLocation == null) {
        return;
      }
      String configStr =
          internalProperties.get(PolarisEntityConstants.getStorageConfigInfoPropertyName());
      if (configStr == null) {
        return;
      }
      PolarisStorageConfigurationInfo storageConfig =
          PolarisStorageConfigurationInfo.deserialize(configStr);
      if (storageConfig == null) {
        return;
      }
      List<String> allowedLocations = storageConfig.getAllowedLocations();
      if (allowedLocations != null && !allowedLocations.isEmpty()) {
        validateBaseLocationAgainstAllowedList(allowedLocations, defaultBaseLocation);
      }
    }

    public Builder setStorageConfigurationInfo(
        RealmConfig realmConfig, StorageConfigInfo storageConfigModel) {
      Preconditions.checkNotNull(
          realmConfig, "realmConfig must be provided when setting StorageConfigInfo");
      this.realmConfig = realmConfig;
      this.storageConfigModel = storageConfigModel;
      return this;
    }

    private void processStorageConfigurationInfo() {
      if (storageConfigModel != null) {
        String defaultBaseLocation = properties.get(DEFAULT_BASE_LOCATION_KEY);
        if (defaultBaseLocation == null) {
          throw new BadRequestException("Must specify default base location");
        }
        List<String> userAllowedLocations = storageConfigModel.getAllowedLocations();
        Set<String> allowedLocations;
        if (userAllowedLocations == null || userAllowedLocations.isEmpty()) {
          allowedLocations = new HashSet<>();
          allowedLocations.add(defaultBaseLocation);
        } else {
          allowedLocations = new HashSet<>(userAllowedLocations);
        }
        validateMaxAllowedLocations(realmConfig, allowedLocations);
        PolarisStorageConfigurationInfo config =
            toStorageConfigurationInfo(
                storageConfigModel, storageConfigModel.getStorageName(), allowedLocations);
        internalProperties.put(
            PolarisEntityConstants.getStorageConfigInfoPropertyName(), config.serialize());
      }
    }

    private static PolarisStorageConfigurationInfo toStorageConfigurationInfo(
        StorageConfigInfo storageConfigModel, String storageName, Set<String> allowedLocations) {
      PolarisStorageConfigurationInfo config;
      switch (storageConfigModel.getStorageType()) {
        case S3:
          config =
              getAwsStorageConfigurationInfo(
                  (AwsStorageConfigInfo) storageConfigModel, storageName, allowedLocations);
          break;
        case AZURE:
          AzureStorageConfigInfo azureConfigModel = (AzureStorageConfigInfo) storageConfigModel;
          config =
              AzureStorageConfigurationInfo.builder()
                  .allowedLocations(allowedLocations)
                  .storageName(storageName)
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
                  .storageName(storageName)
                  .gcpServiceAccount(
                      ((GcpStorageConfigInfo) storageConfigModel).getGcsServiceAccount())
                  .build();
          break;
        case FILE:
          config =
              FileStorageConfigurationInfo.builder()
                  .allowedLocations(allowedLocations)
                  .storageName(storageName)
                  .build();
          break;
        default:
          throw new IllegalStateException(
              "Unsupported storage type: " + storageConfigModel.getStorageType());
      }
      return config;
    }

    public Builder setStorageConfigurationInfos(
        RealmConfig realmConfig, List<StorageConfigInfo> namedStorageConfigModels) {
      Preconditions.checkNotNull(
          realmConfig, "realmConfig must be provided when setting StorageConfigInfos");
      this.realmConfig = realmConfig;
      this.namedStorageConfigModels = namedStorageConfigModels;
      return this;
    }

    /**
     * Processes the named-config array (design.md D6a): {@code null} (never supplied) leaves {@code
     * internalProperties} untouched, so an update omitting the field keeps the set carried forward
     * by {@code Builder(CatalogEntity original)}; an empty list removes the key entirely rather
     * than persisting {@code "{}"}; a non-empty list validates and replaces the whole set.
     */
    private void processStorageConfigurationInfos() {
      if (namedStorageConfigModels == null) {
        return;
      }
      if (namedStorageConfigModels.isEmpty()) {
        internalProperties.remove(PolarisEntityConstants.getStorageConfigInfosPropertyName());
        return;
      }

      Map<String, PolarisStorageConfigurationInfo> namedConfigs = new LinkedHashMap<>();
      for (StorageConfigInfo model : namedStorageConfigModels) {
        String rawName = model.getStorageName();
        if (rawName == null || rawName.isBlank()) {
          throw new BadRequestException(
              "Each entry in storageConfigInfos must have a non-empty storageName");
        }
        String name = rawName.trim();
        if (!STORAGE_NAME_PATTERN.matcher(name).matches()) {
          throw new IllegalArgumentException(
              String.format(
                  "Invalid storage configuration name '%s': must match %s after trimming",
                  name, STORAGE_NAME_PATTERN.pattern()));
        }
        // Names are compared for uniqueness exactly as written, without case folding.
        if (namedConfigs.containsKey(name)) {
          throw new IllegalArgumentException(
              String.format("Duplicate named storage configuration name '%s'", name));
        }

        List<String> userAllowedLocations = model.getAllowedLocations();
        if (userAllowedLocations == null || userAllowedLocations.isEmpty()) {
          // Unlike the default config, a named entry has no catalog base location to fall back
          // to, so an absent/empty allowedLocations is rejected rather than defaulted.
          throw new BadRequestException(
              "Named storage configuration '%s' must specify at least one allowed location", name);
        }
        Set<String> allowedLocations = new HashSet<>(userAllowedLocations);
        validateMaxAllowedLocations(realmConfig, allowedLocations);
        namedConfigs.put(name, toStorageConfigurationInfo(model, name, allowedLocations));
      }

      internalProperties.put(
          PolarisEntityConstants.getStorageConfigInfosPropertyName(),
          PolarisStorageConfigurationInfo.serializeMap(namedConfigs));
    }

    @SuppressWarnings("deprecation")
    private static AwsStorageConfigurationInfo getAwsStorageConfigurationInfo(
        AwsStorageConfigInfo awsConfigModel, String storageName, Set<String> allowedLocations) {
      List<String> encryptionKeys = new ArrayList<>(awsConfigModel.getEncryptionKeys());
      for (String allowedKmsKey : awsConfigModel.getAllowedKmsKeys()) {
        if (!encryptionKeys.contains(allowedKmsKey)) {
          encryptionKeys.add(allowedKmsKey);
        }
      }
      String currentKmsKey = awsConfigModel.getCurrentKmsKey();
      if (currentKmsKey != null && !encryptionKeys.contains(currentKmsKey)) {
        encryptionKeys.add(currentKmsKey);
      }
      return AwsStorageConfigurationInfo.builder()
          .allowedLocations(allowedLocations)
          .storageName(storageName)
          .roleARN(awsConfigModel.getRoleArn())
          .encryptionKeys(encryptionKeys)
          .decryptionKeys(awsConfigModel.getDecryptionKeys())
          .externalId(awsConfigModel.getExternalId())
          .region(awsConfigModel.getRegion())
          .endpoint(awsConfigModel.getEndpoint())
          .stsEndpoint(awsConfigModel.getStsEndpoint())
          .pathStyleAccess(awsConfigModel.getPathStyleAccess())
          .stsUnavailable(awsConfigModel.getStsUnavailable())
          .endpointInternal(awsConfigModel.getEndpointInternal())
          .kmsUnavailable(awsConfigModel.getKmsUnavailable())
          .build();
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

    /**
     * The default configuration's own storage name must not collide with a named configuration's
     * name. This runs after both have been processed rather than inside either one, because an
     * update may supply only one side while the other is carried forward from the existing entity:
     * checking only while processing the named array would miss an update that renames the default
     * config onto an existing named entry.
     */
    private void validateStorageConfigNamesDistinct() {
      String defaultConfigStr =
          internalProperties.get(PolarisEntityConstants.getStorageConfigInfoPropertyName());
      String namedConfigsStr =
          internalProperties.get(PolarisEntityConstants.getStorageConfigInfosPropertyName());
      if (defaultConfigStr == null || namedConfigsStr == null) {
        return;
      }
      PolarisStorageConfigurationInfo defaultConfig =
          PolarisStorageConfigurationInfo.deserialize(defaultConfigStr);
      if (defaultConfig == null || defaultConfig.getStorageName() == null) {
        return;
      }
      String defaultStorageName = defaultConfig.getStorageName().trim();
      if (PolarisStorageConfigurationInfo.deserializeMap(namedConfigsStr)
          .containsKey(defaultStorageName)) {
        throw new IllegalArgumentException(
            String.format(
                "Named storage configuration name '%s' collides with the catalog's default storage"
                    + " configuration name",
                defaultStorageName));
      }
    }

    @Override
    public CatalogEntity build() {
      processStorageConfigurationInfo();
      processStorageConfigurationInfos();
      validateStorageConfigNamesDistinct();
      validateDefaultBaseLocation();
      return new CatalogEntity(buildBase());
    }
  }
}
