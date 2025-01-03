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
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.FileIO;
import org.apache.polaris.core.PolarisConfigurationStore;
import org.apache.polaris.core.context.RealmId;
import org.apache.polaris.core.persistence.PolarisEntityManager;
import org.apache.polaris.core.persistence.PolarisMetaStoreSession;
import org.apache.polaris.core.persistence.PolarisResolvedPathWrapper;
import org.apache.polaris.core.storage.PolarisCredentialVendor;
import org.apache.polaris.core.storage.PolarisStorageActions;

/** A {@link FileIOFactory} that translates WASB paths to ABFS ones */
@RequestScoped
@Identifier("wasb")
public class WasbTranslatingFileIOFactory implements FileIOFactory {

  private final FileIOFactory defaultFileIOFactory;

  @Inject
  public WasbTranslatingFileIOFactory(
      RealmId realmId,
      PolarisEntityManager entityManager,
      PolarisCredentialVendor credentialVendor,
      PolarisMetaStoreSession metaStoreSession,
      PolarisConfigurationStore configurationStore) {
    defaultFileIOFactory =
        new DefaultFileIOFactory(
            realmId, entityManager, credentialVendor, metaStoreSession, configurationStore);
  }

  @Override
  public FileIO loadFileIO(
      String ioImplClassName,
      Map<String, String> properties,
      TableIdentifier identifier,
      Set<String> tableLocations,
      Set<PolarisStorageActions> storageActions,
      PolarisResolvedPathWrapper resolvedStorageEntity) {
    return new WasbTranslatingFileIO(
        defaultFileIOFactory.loadFileIO(
            ioImplClassName,
            properties,
            identifier,
            tableLocations,
            storageActions,
            resolvedStorageEntity));
  }
}
