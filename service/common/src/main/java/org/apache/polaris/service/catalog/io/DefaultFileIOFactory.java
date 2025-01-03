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
package org.apache.polaris.service.catalog.io;

import io.smallrye.common.annotation.Identifier;
import jakarta.annotation.Nonnull;
import jakarta.enterprise.context.RequestScoped;
import jakarta.inject.Inject;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.FileIO;
import org.apache.polaris.core.PolarisConfiguration;
import org.apache.polaris.core.PolarisConfigurationStore;
import org.apache.polaris.core.context.RealmId;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PolarisEntityConstants;
import org.apache.polaris.core.persistence.PolarisEntityManager;
import org.apache.polaris.core.persistence.PolarisMetaStoreSession;
import org.apache.polaris.core.persistence.PolarisResolvedPathWrapper;
import org.apache.polaris.core.storage.PolarisCredentialVendor;
import org.apache.polaris.core.storage.PolarisStorageActions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A simple FileIOFactory implementation that defers all the work to the Iceberg SDK */
@RequestScoped
@Identifier("default")
public class DefaultFileIOFactory implements FileIOFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultFileIOFactory.class);

  private final RealmId realmId;
  private final PolarisEntityManager entityManager;
  private final PolarisCredentialVendor credentialVendor;
  private final PolarisMetaStoreSession metaStoreSession;
  private final PolarisConfigurationStore configurationStore;

  @Inject
  public DefaultFileIOFactory(
      RealmId realmId,
      PolarisEntityManager entityManager,
      PolarisCredentialVendor credentialVendor,
      PolarisMetaStoreSession metaStoreSession,
      PolarisConfigurationStore configurationStore) {
    this.realmId = realmId;
    this.entityManager = entityManager;
    this.credentialVendor = credentialVendor;
    this.metaStoreSession = metaStoreSession;
    this.configurationStore = configurationStore;
  }

  @Override
  public FileIO loadFileIO(
      String ioImplClassName,
      Map<String, String> properties,
      TableIdentifier identifier,
      Set<String> tableLocations,
      Set<PolarisStorageActions> storageActions,
      PolarisResolvedPathWrapper resolvedStorageEntity) {
    if (resolvedStorageEntity != null) {
      Optional<PolarisEntity> storageInfoEntity =
          findStorageInfoFromHierarchy(resolvedStorageEntity);
      Map<String, String> credentialsMap =
          storageInfoEntity
              .map(
                  storageInfo ->
                      refreshCredentials(identifier, tableLocations, storageActions, storageInfo))
              .orElse(Map.of());

      // Update the FileIO before we write the new metadata file
      // update with properties in case there are table-level overrides the credentials should
      // always override table-level properties, since storage configuration will be found at
      // whatever entity defines it
      properties.putAll(credentialsMap);
    }

    return CatalogUtil.loadFileIO(ioImplClassName, properties, new Configuration());
  }

  private static @Nonnull Optional<PolarisEntity> findStorageInfoFromHierarchy(
      PolarisResolvedPathWrapper resolvedStorageEntity) {
    Optional<PolarisEntity> storageInfoEntity =
        resolvedStorageEntity.getRawFullPath().reversed().stream()
            .filter(
                e ->
                    e.getInternalPropertiesAsMap()
                        .containsKey(PolarisEntityConstants.getStorageConfigInfoPropertyName()))
            .findFirst();
    return storageInfoEntity;
  }

  private Map<String, String> refreshCredentials(
      TableIdentifier tableIdentifier,
      Set<String> tableLocations,
      Set<PolarisStorageActions> storageActions,
      PolarisEntity entity) {
    Boolean skipCredentialSubscopingIndirection =
        getBooleanContextConfiguration(
            PolarisConfiguration.SKIP_CREDENTIAL_SUBSCOPING_INDIRECTION.key,
            PolarisConfiguration.SKIP_CREDENTIAL_SUBSCOPING_INDIRECTION.defaultValue);
    if (Boolean.TRUE.equals(skipCredentialSubscopingIndirection)) {
      LOGGER
          .atInfo()
          .addKeyValue("tableIdentifier", tableIdentifier)
          .log("Skipping generation of subscoped creds for table");
      return Map.of();
    }

    boolean allowList =
        storageActions.contains(PolarisStorageActions.LIST)
            || storageActions.contains(PolarisStorageActions.ALL);
    Set<String> writeLocations =
        storageActions.contains(PolarisStorageActions.WRITE)
                || storageActions.contains(PolarisStorageActions.DELETE)
                || storageActions.contains(PolarisStorageActions.ALL)
            ? tableLocations
            : Set.of();
    Map<String, String> credentialsMap =
        entityManager
            .getCredentialCache()
            .getOrGenerateSubScopeCreds(
                credentialVendor,
                metaStoreSession,
                entity,
                allowList,
                tableLocations,
                writeLocations);
    LOGGER
        .atDebug()
        .addKeyValue("tableIdentifier", tableIdentifier)
        .addKeyValue("credentialKeys", credentialsMap.keySet())
        .log("Loaded scoped credentials for table");
    if (credentialsMap.isEmpty()) {
      LOGGER.debug("No credentials found for table");
    }
    return credentialsMap;
  }

  private Boolean getBooleanContextConfiguration(String configKey, boolean defaultValue) {
    return configurationStore.getConfiguration(realmId, configKey, defaultValue);
  }
}
