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
import jakarta.enterprise.context.RequestScoped;
import jakarta.inject.Inject;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.FileIO;
import org.apache.polaris.core.PolarisConfigurationStore;
import org.apache.polaris.core.context.RealmId;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.persistence.PolarisEntityManager;
import org.apache.polaris.core.persistence.PolarisMetaStoreSession;
import org.apache.polaris.core.persistence.PolarisResolvedPathWrapper;
import org.apache.polaris.core.storage.PolarisCredentialVendor;
import org.apache.polaris.core.storage.PolarisStorageActions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A default FileIO factory implementation for creating Iceberg {@link FileIO} instances with
 * contextual table-level properties.
 *
 * <p>This class acts as a translation layer between Polaris properties and the properties required
 * by Iceberg's {@link FileIO}. For example, it evaluates storage actions and retrieves subscoped
 * credentials to initialize a {@link FileIO} instance with the most limited permissions necessary.
 */
@RequestScoped
@Identifier("default")
public class DefaultFileIOFactory implements FileIOFactory {

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
    properties = new HashMap<>(properties);

    if (resolvedStorageEntity != null) {
      // Get subcoped creds
      Optional<PolarisEntity> storageInfoEntity =
          FileIOUtil.findStorageInfoFromHierarchy(resolvedStorageEntity);
      Map<String, String> credentialsMap =
          storageInfoEntity
              .map(
                  storageInfo ->
                      FileIOUtil.refreshCredentials(
                          realmId,
                          entityManager,
                          credentialVendor,
                          metaStoreSession,
                          configurationStore,
                          identifier,
                          tableLocations,
                          storageActions,
                          storageInfo))
              .orElse(Map.of());

      // Update the FileIO before we write the new metadata file
      // update with properties in case there are table-level overrides the credentials should
      // always override table-level properties, since storage configuration will be found at
      // whatever entity defines it
      properties.putAll(credentialsMap);
    }

    return CatalogUtil.loadFileIO(ioImplClassName, properties, new Configuration());
  }
}
