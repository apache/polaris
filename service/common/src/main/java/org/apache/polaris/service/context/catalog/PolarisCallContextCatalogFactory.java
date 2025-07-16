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
package org.apache.polaris.service.context.catalog;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.SecurityContext;
import java.util.HashMap;
import java.util.Map;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.polaris.core.auth.AuthenticatedPolarisPrincipal;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.entity.CatalogEntity;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.persistence.MetaStoreManagerFactory;
import org.apache.polaris.core.persistence.PolarisEntityManager;
import org.apache.polaris.core.persistence.resolver.PolarisResolutionManifest;
import org.apache.polaris.core.secrets.UserSecretsManagerFactory;
import org.apache.polaris.service.catalog.iceberg.IcebergCatalog;
import org.apache.polaris.service.catalog.io.FileIOFactory;
import org.apache.polaris.service.config.RealmEntityManagerFactory;
import org.apache.polaris.service.events.PolarisEventListener;
import org.apache.polaris.service.task.TaskExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ApplicationScoped
public class PolarisCallContextCatalogFactory implements CallContextCatalogFactory {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(PolarisCallContextCatalogFactory.class);

  private final RealmEntityManagerFactory entityManagerFactory;
  private final TaskExecutor taskExecutor;
  private final FileIOFactory fileIOFactory;
  private final MetaStoreManagerFactory metaStoreManagerFactory;
  private final UserSecretsManagerFactory userSecretsManagerFactory;
  private final PolarisEventListener polarisEventListener;

  @Inject
  public PolarisCallContextCatalogFactory(
      RealmEntityManagerFactory entityManagerFactory,
      MetaStoreManagerFactory metaStoreManagerFactory,
      UserSecretsManagerFactory userSecretsManagerFactory,
      TaskExecutor taskExecutor,
      FileIOFactory fileIOFactory,
      PolarisEventListener polarisEventListener) {
    this.entityManagerFactory = entityManagerFactory;
    this.metaStoreManagerFactory = metaStoreManagerFactory;
    this.userSecretsManagerFactory = userSecretsManagerFactory;
    this.taskExecutor = taskExecutor;
    this.fileIOFactory = fileIOFactory;
    this.polarisEventListener = polarisEventListener;
  }

  @Override
  public Catalog createCallContextCatalog(
      CallContext context,
      AuthenticatedPolarisPrincipal authenticatedPrincipal,
      SecurityContext securityContext,
      final PolarisResolutionManifest resolvedManifest) {
    PolarisBaseEntity baseCatalogEntity =
        resolvedManifest.getResolvedReferenceCatalogEntity().getRawLeafEntity();
    String catalogName = baseCatalogEntity.getName();

    String realm = context.getRealmContext().getRealmIdentifier();
    String catalogKey = realm + "/" + catalogName;
    LOGGER.debug("Initializing new BasePolarisCatalog for key: {}", catalogKey);

    PolarisEntityManager entityManager =
        entityManagerFactory.getOrCreateEntityManager(context.getRealmContext());

    IcebergCatalog catalogInstance =
        new IcebergCatalog(
            entityManager,
            metaStoreManagerFactory.getOrCreateMetaStoreManager(context.getRealmContext()),
            context,
            resolvedManifest,
            securityContext,
            taskExecutor,
            fileIOFactory,
            polarisEventListener);

    CatalogEntity catalog = CatalogEntity.of(baseCatalogEntity);
    Map<String, String> catalogProperties = new HashMap<>(catalog.getPropertiesAsMap());
    String defaultBaseLocation = catalog.getBaseLocation();
    LOGGER.debug(
        "Looked up defaultBaseLocation {} for catalog {}", defaultBaseLocation, catalogKey);

    if (defaultBaseLocation == null) {
      throw new IllegalStateException(
          String.format(
              "Catalog '%s' does not have a configured warehouse location. "
                  + "Please configure a default base location for this catalog.",
              catalogKey));
    }

    catalogProperties.put(CatalogProperties.WAREHOUSE_LOCATION, defaultBaseLocation);

    // TODO: The initialize properties might need to take more from CallContext and the
    // CatalogEntity.
    catalogInstance.initialize(catalogName, catalogProperties);

    return catalogInstance;
  }
}
