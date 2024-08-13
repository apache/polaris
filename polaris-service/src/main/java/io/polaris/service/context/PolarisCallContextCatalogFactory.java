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
package io.polaris.service.context;

import io.polaris.core.auth.AuthenticatedPolarisPrincipal;
import io.polaris.core.context.CallContext;
import io.polaris.core.entity.CatalogEntity;
import io.polaris.core.entity.PolarisBaseEntity;
import io.polaris.core.persistence.PolarisEntityManager;
import io.polaris.core.persistence.resolver.PolarisResolutionManifest;
import io.polaris.service.catalog.BasePolarisCatalog;
import io.polaris.service.config.RealmEntityManagerFactory;
import io.polaris.service.task.TaskExecutor;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.Catalog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PolarisCallContextCatalogFactory implements CallContextCatalogFactory {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(PolarisCallContextCatalogFactory.class);

  private static final String WAREHOUSE_LOCATION_BASEDIR =
      "/tmp/iceberg_rest_server_warehouse_data/";

  private final RealmEntityManagerFactory entityManagerFactory;
  private final TaskExecutor taskExecutor;

  public PolarisCallContextCatalogFactory(
      RealmEntityManagerFactory entityManagerFactory, TaskExecutor taskExecutor) {
    this.entityManagerFactory = entityManagerFactory;
    this.taskExecutor = taskExecutor;
  }

  @Override
  public Catalog createCallContextCatalog(
      CallContext context,
      AuthenticatedPolarisPrincipal authenticatedPrincipal,
      final PolarisResolutionManifest resolvedManifest) {
    PolarisBaseEntity baseCatalogEntity =
        resolvedManifest.getResolvedReferenceCatalogEntity().getRawLeafEntity();
    String catalogName = baseCatalogEntity.getName();

    String realm = context.getRealmContext().getRealmIdentifier();
    String catalogKey = realm + "/" + catalogName;
    LOGGER.info("Initializing new BasePolarisCatalog for key: {}", catalogKey);

    PolarisEntityManager entityManager =
        entityManagerFactory.getOrCreateEntityManager(context.getRealmContext());

    BasePolarisCatalog catalogInstance =
        new BasePolarisCatalog(
            entityManager, context, resolvedManifest, authenticatedPrincipal, taskExecutor);

    context.contextVariables().put(CallContext.REQUEST_PATH_CATALOG_INSTANCE_KEY, catalogInstance);

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
