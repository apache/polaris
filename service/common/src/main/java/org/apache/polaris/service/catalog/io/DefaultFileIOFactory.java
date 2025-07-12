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

import com.google.common.annotations.VisibleForTesting;
import io.smallrye.common.annotation.Identifier;
import jakarta.annotation.Nonnull;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.FileIO;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.persistence.MetaStoreManagerFactory;
import org.apache.polaris.core.persistence.PolarisEntityManager;
import org.apache.polaris.core.persistence.PolarisResolvedPathWrapper;
import org.apache.polaris.core.storage.AccessConfig;
import org.apache.polaris.core.storage.PolarisCredentialVendor;
import org.apache.polaris.core.storage.PolarisStorageActions;
import org.apache.polaris.service.config.RealmEntityManagerFactory;

/**
 * A default FileIO factory implementation for creating Iceberg {@link FileIO} instances with
 * contextual table-level properties.
 *
 * <p>This class acts as a translation layer between Polaris properties and the properties required
 * by Iceberg's {@link FileIO}. For example, it evaluates storage actions and retrieves subscoped
 * credentials to initialize a {@link FileIO} instance with the most limited permissions necessary.
 */
@ApplicationScoped
@Identifier("default")
public class DefaultFileIOFactory implements FileIOFactory {

  private final RealmEntityManagerFactory realmEntityManagerFactory;
  private final MetaStoreManagerFactory metaStoreManagerFactory;

  @Inject
  public DefaultFileIOFactory(
      RealmEntityManagerFactory realmEntityManagerFactory,
      MetaStoreManagerFactory metaStoreManagerFactory) {
    this.realmEntityManagerFactory = realmEntityManagerFactory;
    this.metaStoreManagerFactory = metaStoreManagerFactory;
  }

  @Override
  public FileIO loadFileIO(
      @Nonnull CallContext callContext,
      @Nonnull String ioImplClassName,
      @Nonnull Map<String, String> properties,
      @Nonnull TableIdentifier identifier,
      @Nonnull Set<String> tableLocations,
      @Nonnull Set<PolarisStorageActions> storageActions,
      @Nonnull PolarisResolvedPathWrapper resolvedEntityPath) {
    RealmContext realmContext = callContext.getRealmContext();
    PolarisEntityManager entityManager =
        realmEntityManagerFactory.getOrCreateEntityManager(realmContext);
    PolarisCredentialVendor credentialVendor =
        metaStoreManagerFactory.getOrCreateMetaStoreManager(realmContext);

    // Get subcoped creds
    properties = new HashMap<>(properties);
    Optional<PolarisEntity> storageInfoEntity =
        FileIOUtil.findStorageInfoFromHierarchy(resolvedEntityPath);
    Optional<AccessConfig> accessConfig =
        storageInfoEntity.map(
            storageInfo ->
                FileIOUtil.refreshAccessConfig(
                    callContext,
                    entityManager,
                    credentialVendor,
                    identifier,
                    tableLocations,
                    storageActions,
                    storageInfo));

    // Update the FileIO with the subscoped credentials
    // Update with properties in case there are table-level overrides the credentials should
    // always override table-level properties, since storage configuration will be found at
    // whatever entity defines it
    if (accessConfig.isPresent()) {
      properties.putAll(accessConfig.get().credentials());
      properties.putAll(accessConfig.get().extraProperties());
    }

    return loadFileIOInternal(ioImplClassName, properties);
  }

  @VisibleForTesting
  FileIO loadFileIOInternal(
      @Nonnull String ioImplClassName, @Nonnull Map<String, String> properties) {
    return new ExceptionMappingFileIO(
        CatalogUtil.loadFileIO(ioImplClassName, properties, new Configuration()));
  }
}
