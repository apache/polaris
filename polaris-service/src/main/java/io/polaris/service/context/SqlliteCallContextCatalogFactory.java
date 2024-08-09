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
import io.polaris.core.persistence.resolver.PolarisResolutionManifest;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.catalog.Catalog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * For local/dev testing, this RealmCallContextFactory uses Sqllite as the backing store for Catalog
 * metadata using local-filesystem-based files as the persistence layer.
 *
 * <p>Realms will reside in different subdirectories under a shared base directory on the local
 * filesystem. Each Catalog in the realm will be a different sqllite file.
 */
public class SqlliteCallContextCatalogFactory implements CallContextCatalogFactory {
  private static final String DEFAULT_METASTORE_STATE_BASEDIR =
      "/tmp/iceberg_rest_server_sqlitestate_basedir/";
  private static final String WAREHOUSE_LOCATION_BASEDIR =
      "/tmp/iceberg_rest_server_warehouse_data/";

  private static final Logger LOGGER =
      LoggerFactory.getLogger(SqlliteCallContextCatalogFactory.class);

  private final Map<String, Catalog> cachedCatalogs = new HashMap<>();
  private final Map<String, String> catalogBaseDirs;

  public SqlliteCallContextCatalogFactory(Map<String, String> catalogBaseDirs) {
    this.catalogBaseDirs = catalogBaseDirs;
  }

  @Override
  public Catalog createCallContextCatalog(
      CallContext context,
      AuthenticatedPolarisPrincipal polarisPrincipal,
      PolarisResolutionManifest resolvedManifest) {
    String catalogName =
        resolvedManifest.getResolvedReferenceCatalogEntity().getRawLeafEntity().getName();
    if (catalogName == null) {
      catalogName = "default";
    }

    String realm = context.getRealmContext().getRealmIdentifier();
    String catalogKey = realm + "/" + catalogName;
    LOGGER.debug("Looking up catalogKey: {}", catalogKey);

    Catalog catalogInstance = cachedCatalogs.get(catalogKey);
    if (catalogInstance == null) {
      Map<String, String> catalogProperties = new HashMap<>();
      catalogProperties.put(CatalogProperties.CATALOG_IMPL, "org.apache.iceberg.jdbc.JdbcCatalog");
      catalogProperties.put("jdbc.schema-version", "V1");

      // TODO: Do sanitization in case this ever runs in an exposed environment to avoid
      // injection attacks.
      String baseDir = catalogBaseDirs.getOrDefault(realm, DEFAULT_METASTORE_STATE_BASEDIR);

      String realmDir = Paths.get(baseDir, realm).toString();
      String catalogFile = Paths.get(realmDir, catalogName).toString();

      // Ensure parent directories of metastore-state base directory exists.
      LOGGER.info("Creating metastore state directory: {}", realmDir);
      try {
        Path result = Files.createDirectories(FileSystems.getDefault().getPath(realmDir));
      } catch (IOException ioe) {
        throw new RuntimeException(ioe);
      }

      catalogProperties.put(CatalogProperties.URI, "jdbc:sqlite:file:" + catalogFile);

      // TODO: Derive warehouse location from realm configs.
      catalogProperties.put(
          CatalogProperties.WAREHOUSE_LOCATION,
          Paths.get(WAREHOUSE_LOCATION_BASEDIR, catalogKey).toString());

      catalogInstance =
          CatalogUtil.buildIcebergCatalog(
              "catalog_" + catalogKey, catalogProperties, new Configuration());
      cachedCatalogs.put(catalogKey, catalogInstance);
    }
    return catalogInstance;
  }
}
