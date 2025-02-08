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
package org.apache.polaris.service.context;

import jakarta.enterprise.context.RequestScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.SecurityContext;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.polaris.core.PolarisConfigurationStore;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.auth.AuthenticatedPolarisPrincipal;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.entity.CatalogEntity;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.persistence.PolarisEntityManager;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.core.persistence.PolarisMetaStoreSession;
import org.apache.polaris.core.persistence.resolver.PolarisResolutionManifest;
import org.apache.polaris.service.catalog.BasePolarisCatalog;
import org.apache.polaris.service.catalog.io.FileIOFactory;
import org.apache.polaris.service.task.TaskExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RequestScoped
public class PolarisCallContextCatalogFactory implements CallContextCatalogFactory {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(PolarisCallContextCatalogFactory.class);

  private static final String WAREHOUSE_LOCATION_BASEDIR =
      "/tmp/iceberg_rest_server_warehouse_data/";

  private final PolarisEntityManager entityManager;
  private final PolarisMetaStoreManager metaStoreManager;
  private final PolarisMetaStoreSession metaStoreSession;
  private final PolarisConfigurationStore configurationStore;
  private final PolarisDiagnostics diagnostics;
  private final TaskExecutor taskExecutor;
  private final FileIOFactory fileIOFactory;

  @Inject
  public PolarisCallContextCatalogFactory(
      PolarisEntityManager entityManager,
      PolarisMetaStoreManager metaStoreManager,
      PolarisMetaStoreSession metaStoreSession,
      PolarisConfigurationStore configurationStore,
      PolarisDiagnostics diagnostics,
      TaskExecutor taskExecutor,
      FileIOFactory fileIOFactory) {
    this.entityManager = entityManager;
    this.metaStoreManager = metaStoreManager;
    this.metaStoreSession = metaStoreSession;
    this.configurationStore = configurationStore;
    this.diagnostics = diagnostics;
    this.taskExecutor = taskExecutor;
    this.fileIOFactory = fileIOFactory;
  }

  @Override
  public Catalog createCallContextCatalog(
      RealmContext realmContext,
      AuthenticatedPolarisPrincipal authenticatedPrincipal,
      SecurityContext securityContext,
      final PolarisResolutionManifest resolvedManifest) {
    PolarisBaseEntity baseCatalogEntity =
        resolvedManifest.getResolvedReferenceCatalogEntity().getRawLeafEntity();
    String catalogName = baseCatalogEntity.getName();

    String realm = realmContext.getRealmIdentifier();
    String catalogKey = realm + "/" + catalogName;
    LOGGER.info("Initializing new BasePolarisCatalog for key: {}", catalogKey);

    BasePolarisCatalog catalogInstance =
        new BasePolarisCatalog(
            realmContext,
            entityManager,
            metaStoreManager,
            metaStoreSession,
            configurationStore,
            diagnostics,
            resolvedManifest,
            securityContext,
            taskExecutor,
            fileIOFactory);

    CatalogEntity catalog = CatalogEntity.of(baseCatalogEntity);
    Map<String, String> catalogProperties = new HashMap<>(catalog.getPropertiesAsMap());
    String defaultBaseLocation = catalog.getDefaultBaseLocation();
    LOGGER.info("Looked up defaultBaseLocation {} for catalog {}", defaultBaseLocation, catalogKey);
    catalogProperties.put(
        CatalogProperties.WAREHOUSE_LOCATION,
        Objects.requireNonNullElseGet(
            defaultBaseLocation,
            () -> Paths.get(WAREHOUSE_LOCATION_BASEDIR, catalogKey).toString()));

    // TODO: The initialize properties might need to take more from CallContext and the
    // CatalogEntity.
    catalogInstance.initialize(catalogName, catalogProperties);

    return catalogInstance;
  }
}
