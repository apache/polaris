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
package org.apache.polaris.spark;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.arrow.util.VisibleForTesting;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.rest.auth.OAuth2Util;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.Tasks;
import org.apache.polaris.spark.utils.DeltaHelper;
import org.apache.polaris.spark.utils.HudiHelper;
import org.apache.polaris.spark.utils.PaimonHelper;
import org.apache.polaris.spark.utils.PolarisCatalogUtils;
import org.apache.spark.sql.catalyst.analysis.NamespaceAlreadyExistsException;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.analysis.NoSuchViewException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.apache.spark.sql.catalyst.analysis.ViewAlreadyExistsException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.NamespaceChange;
import org.apache.spark.sql.connector.catalog.StagedTable;
import org.apache.spark.sql.connector.catalog.StagingTableCatalog;
import org.apache.spark.sql.connector.catalog.SupportsNamespaces;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.catalog.TableChange;
import org.apache.spark.sql.connector.catalog.View;
import org.apache.spark.sql.connector.catalog.ViewChange;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SparkCatalog implementation that is able to interact with both Iceberg SparkCatalog and Polaris.
 */
public abstract class BasePolarisSparkCatalog
    implements StagingTableCatalog, TableCatalog, SupportsNamespaces {

  private static final Logger LOG = LoggerFactory.getLogger(BasePolarisSparkCatalog.class);
  private static final int MAX_POLARIS_REGISTRATION_RETRIES = 3;
  private static final long RETRY_MIN_WAIT_MS = 1000L;
  private static final long RETRY_MAX_WAIT_MS = 5000L;
  private static final long RETRY_TOTAL_TIMEOUT_MS = 30000L;

  @VisibleForTesting protected String catalogName = null;
  @VisibleForTesting protected org.apache.iceberg.spark.SparkCatalog icebergsSparkCatalog = null;
  @VisibleForTesting protected PolarisSparkCatalog polarisSparkCatalog = null;
  @VisibleForTesting protected DeltaHelper deltaHelper = null;
  @VisibleForTesting protected HudiHelper hudiHelper = null;
  @VisibleForTesting protected PaimonHelper paimonHelper = null;

  protected abstract PolarisCatalogUtils createCatalogUtils();

  @Override
  public String name() {
    return catalogName;
  }

  /**
   * Check whether invalid catalog configuration is provided, and return an option map with catalog
   * type configured correctly. This function mainly validates two parts: 1) No customized catalog
   * implementation is provided. 2) No non-rest catalog type is configured.
   */
  @VisibleForTesting
  public CaseInsensitiveStringMap validateAndResolveCatalogOptions(
      CaseInsensitiveStringMap options) {
    Preconditions.checkArgument(
        options.get(CatalogProperties.CATALOG_IMPL) == null,
        "Customized catalog implementation is not supported and not needed, please remove the configuration!");

    String catalogType =
        PropertyUtil.propertyAsString(
            options, CatalogUtil.ICEBERG_CATALOG_TYPE, CatalogUtil.ICEBERG_CATALOG_TYPE_REST);
    Preconditions.checkArgument(
        catalogType.equals(CatalogUtil.ICEBERG_CATALOG_TYPE_REST),
        "Only rest catalog type is allowed, but got catalog type: "
            + catalogType
            + ". Either configure the type to rest or remove the config");

    Map<String, String> resolvedOptions = Maps.newHashMap();
    resolvedOptions.putAll(options);
    // when no catalog type is configured, iceberg uses hive by default. Here, we make sure the
    // type is set to rest since we only support rest catalog.
    resolvedOptions.put(CatalogUtil.ICEBERG_CATALOG_TYPE, CatalogUtil.ICEBERG_CATALOG_TYPE_REST);

    return new CaseInsensitiveStringMap(resolvedOptions);
  }

  /**
   * Initialize REST Catalog for Iceberg and Polaris, this is the only catalog type supported by
   * Polaris at this moment.
   */
  private void initRESTCatalog(String name, CaseInsensitiveStringMap options) {
    CaseInsensitiveStringMap resolvedOptions = validateAndResolveCatalogOptions(options);

    // initialize the icebergSparkCatalog
    this.icebergsSparkCatalog = new org.apache.iceberg.spark.SparkCatalog();
    this.icebergsSparkCatalog.initialize(name, resolvedOptions);

    // initialize the polaris spark catalog
    OAuth2Util.AuthSession catalogAuth =
        PolarisCatalogUtils.getAuthSession(this.icebergsSparkCatalog);
    PolarisRESTCatalog restCatalog = new PolarisRESTCatalog();
    restCatalog.initialize(options, catalogAuth);
    this.polarisSparkCatalog = new PolarisSparkCatalog(restCatalog, createCatalogUtils());
    this.polarisSparkCatalog.initialize(name, resolvedOptions);
  }

  @Override
  public void initialize(String name, CaseInsensitiveStringMap options) {
    this.catalogName = name;
    initRESTCatalog(name, options);
    this.deltaHelper = new DeltaHelper(options);
    this.hudiHelper = new HudiHelper(options);
    this.paimonHelper = new PaimonHelper(name, options);
  }

  @Override
  public Table loadTable(Identifier ident) throws NoSuchTableException {
    try {
      return this.icebergsSparkCatalog.loadTable(ident);
    } catch (NoSuchTableException e) {
      // Not an Iceberg table; resolve the Generic Table provider before delegating.
    }

    String provider = this.polarisSparkCatalog.getTableFormat(ident);
    if (PolarisCatalogUtils.usePaimon(provider)) {
      try {
        return paimonHelper.loadPaimonCatalog().loadTable(ident);
      } catch (NoSuchTableException e) {
        throw e;
      } catch (Exception e) {
        throw new RuntimeException(
            String.format("Failed to load Paimon table %s: %s", ident, e.getMessage()), e);
      }
    }
    return this.polarisSparkCatalog.loadTable(ident);
  }

  @Override
  @SuppressWarnings({"deprecation", "RedundantSuppression"})
  public Table createTable(
      Identifier ident, StructType schema, Transform[] transforms, Map<String, String> properties)
      throws TableAlreadyExistsException, NoSuchNamespaceException {
    String provider = properties.get(PolarisCatalogUtils.TABLE_PROVIDER_KEY);
    if (PolarisCatalogUtils.useIceberg(provider)) {
      return this.icebergsSparkCatalog.createTable(ident, schema, transforms, properties);
    } else if (PolarisCatalogUtils.usePaimon(provider)) {
      TableCatalog paimonCatalog = paimonHelper.loadPaimonCatalog();
      paimonHelper.ensureNamespaceExists(ident.namespace());
      Table paimonTable = paimonCatalog.createTable(ident, schema, transforms, properties);

      // Every failure after physical creation must roll the Paimon table back exactly once.
      try {
        String tableLocation = paimonTable.properties().get(TableCatalog.PROP_LOCATION);
        if (tableLocation == null) {
          throw new IllegalStateException(
              String.format(
                  "Paimon table %s was created but its properties do not contain a location. "
                      + "Paimon should populate it from CoreOptions.PATH.",
                  ident));
        }

        Map<String, String> registrationProperties = Maps.newHashMap(properties);
        registrationProperties.put(TableCatalog.PROP_LOCATION, tableLocation);
        registerPaimonTable(ident, registrationProperties);
      } catch (Exception e) {
        LOG.error(
            "Failed to finish creating Paimon table {}, rolling back physical table creation: {}",
            ident,
            e.getMessage());
        rollbackPaimonTableCreation(ident);
        if (e instanceof TableAlreadyExistsException tableAlreadyExistsException) {
          throw tableAlreadyExistsException;
        } else if (e instanceof NoSuchNamespaceException noSuchNamespaceException) {
          throw noSuchNamespaceException;
        } else if (e instanceof RuntimeException runtimeException) {
          throw runtimeException;
        }
        throw new RuntimeException(e);
      }
      return paimonTable;
    } else {
      if (PolarisCatalogUtils.isTableWithSparkManagedLocation(properties)) {
        throw new UnsupportedOperationException(
            "Create table without location key is not supported by Polaris. Please provide location or path on table creation.");
      }
      if (PolarisCatalogUtils.useDelta(provider)) {
        // For delta table, we load the delta catalog to help dealing with the
        // delta log creation.
        TableCatalog deltaCatalog = deltaHelper.loadDeltaCatalog(this.polarisSparkCatalog);
        return deltaCatalog.createTable(ident, schema, transforms, properties);
      } else if (PolarisCatalogUtils.useHudi(provider)) {
        // For creating the hudi table, we load HoodieCatalog
        // to create the .hoodie folder in cloud storage
        TableCatalog hudiCatalog = hudiHelper.loadHudiCatalog(this.polarisSparkCatalog);
        return hudiCatalog.createTable(ident, schema, transforms, properties);
      } else {
        return this.polarisSparkCatalog.createTable(ident, schema, transforms, properties);
      }
    }
  }

  private void registerPaimonTable(Identifier ident, Map<String, String> properties)
      throws TableAlreadyExistsException, NoSuchNamespaceException {
    try {
      Tasks.foreach(properties)
          .retry(MAX_POLARIS_REGISTRATION_RETRIES)
          .exponentialBackoff(RETRY_MIN_WAIT_MS, RETRY_MAX_WAIT_MS, RETRY_TOTAL_TIMEOUT_MS, 2.0)
          .shouldRetryTest(
              e ->
                  !(e instanceof TableAlreadyExistsException)
                      && !(e instanceof NoSuchNamespaceException))
          .throwFailureWhenFinished()
          .run(
              registrationProperties -> {
                String format =
                    registrationProperties.get(PolarisCatalogUtils.TABLE_PROVIDER_KEY);
                String baseLocation = registrationProperties.get(TableCatalog.PROP_LOCATION);
                this.polarisSparkCatalog.registerGenericTable(
                    ident, format, baseLocation, registrationProperties);
              },
              Exception.class);
    } catch (TableAlreadyExistsException | NoSuchNamespaceException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(
          String.format(
              "Failed to register Paimon table %s in Polaris after %d attempts",
              ident, MAX_POLARIS_REGISTRATION_RETRIES + 1),
          e);
    }
  }

  @Override
  public Table alterTable(Identifier ident, TableChange... changes) throws NoSuchTableException {
    try {
      return this.icebergsSparkCatalog.alterTable(ident, changes);
    } catch (NoSuchTableException e) {
      // Not an Iceberg table; resolve the Generic Table provider before delegating.
    }

    String provider = this.polarisSparkCatalog.getTableFormat(ident);
    if (PolarisCatalogUtils.useDelta(provider)) {
      TableCatalog deltaCatalog = deltaHelper.loadDeltaCatalog(this.polarisSparkCatalog);
      return deltaCatalog.alterTable(ident, changes);
    } else if (PolarisCatalogUtils.useHudi(provider)) {
      TableCatalog hudiCatalog = hudiHelper.loadHudiCatalog(this.polarisSparkCatalog);
      return hudiCatalog.alterTable(ident, changes);
    } else if (PolarisCatalogUtils.usePaimon(provider)) {
      try {
        return paimonHelper.loadPaimonCatalog().alterTable(ident, changes);
      } catch (NoSuchTableException e) {
        throw e;
      } catch (Exception e) {
        throw new RuntimeException(
            String.format("Failed to alter Paimon table %s: %s", ident, e.getMessage()), e);
      }
    }
    return this.polarisSparkCatalog.alterTable(ident);
  }

  @Override
  public boolean dropTable(Identifier ident) {
    if (this.icebergsSparkCatalog.dropTable(ident)) {
      return true;
    }

    try {
      String provider = this.polarisSparkCatalog.getTableFormat(ident);
      if (PolarisCatalogUtils.usePaimon(provider)) {
        try {
          paimonHelper.loadPaimonCatalog().dropTable(ident);
        } catch (Exception e) {
          LOG.error(
              "Failed to drop table {} from Paimon catalog. "
                  + "Polaris metadata will not be removed to maintain consistency: {}",
              ident,
              e.getMessage());
          throw new RuntimeException(
              String.format("Failed to drop Paimon table %s: %s", ident, e.getMessage()), e);
        }

        boolean droppedFromPolaris = this.polarisSparkCatalog.dropTable(ident);
        if (!droppedFromPolaris) {
          LOG.warn(
              "Paimon table {} data was dropped but Polaris metadata removal failed. "
                  + "The stale Polaris entry may need manual cleanup.",
              ident);
        }
        return droppedFromPolaris;
      }
      return this.polarisSparkCatalog.dropTable(ident);
    } catch (NoSuchTableException e) {
      LOG.warn("Table {} not found in Polaris catalog during drop operation", ident);
      return false;
    }
  }

  private void rollbackPaimonTableCreation(Identifier ident) {
    try {
      paimonHelper.loadPaimonCatalog().dropTable(ident);
      LOG.info("Successfully rolled back Paimon table creation for {}", ident);
    } catch (Exception e) {
      LOG.error(
          "Failed to roll back Paimon table {} creation. Manual cleanup may be required: {}",
          ident,
          e.getMessage());
    }
  }

  @Override
  public void renameTable(Identifier from, Identifier to)
      throws NoSuchTableException, TableAlreadyExistsException {
    try {
      this.icebergsSparkCatalog.renameTable(from, to);
    } catch (NoSuchTableException e) {
      this.polarisSparkCatalog.renameTable(from, to);
    }
  }

  @Override
  public void invalidateTable(Identifier ident) {
    this.icebergsSparkCatalog.invalidateTable(ident);
  }

  @Override
  public boolean purgeTable(Identifier ident) {
    String provider;
    try {
      provider = this.polarisSparkCatalog.getTableFormat(ident);
    } catch (NoSuchTableException e) {
      return this.icebergsSparkCatalog.purgeTable(ident);
    }

    if (PolarisCatalogUtils.usePaimon(provider)) {
      // Paimon's Spark catalog has no distinct purge API; dropping it removes the physical table.
      try {
        paimonHelper.loadPaimonCatalog().dropTable(ident);
      } catch (Exception e) {
        LOG.error(
            "Failed to purge table {} from Paimon catalog. "
                + "Polaris metadata will not be removed to maintain consistency: {}",
            ident,
            e.getMessage());
        throw new RuntimeException(
            String.format("Failed to purge Paimon table %s: %s", ident, e.getMessage()), e);
      }

      boolean purgedFromPolaris;
      try {
        purgedFromPolaris = this.polarisSparkCatalog.purgeTable(ident);
      } catch (RuntimeException e) {
        LOG.error(
            "Paimon table {} data was purged but Polaris metadata removal failed with an "
                + "exception. The stale Polaris entry may need manual cleanup: {}",
            ident,
            e.getMessage());
        throw e;
      }
      if (!purgedFromPolaris) {
        LOG.warn(
            "Paimon table {} data was purged but Polaris metadata removal failed. "
                + "The stale Polaris entry may need manual cleanup.",
            ident);
      }
      return purgedFromPolaris;
    }
    return this.polarisSparkCatalog.purgeTable(ident);
  }

  @Override
  public Identifier[] listTables(String[] namespace) {
    Identifier[] icebergIdents = this.icebergsSparkCatalog.listTables(namespace);
    Identifier[] genericTableIdents = this.polarisSparkCatalog.listTables(namespace);

    return Stream.concat(Arrays.stream(icebergIdents), Arrays.stream(genericTableIdents))
        .toArray(Identifier[]::new);
  }

  @Override
  @SuppressWarnings({"deprecation", "RedundantSuppression"})
  public StagedTable stageCreate(
      Identifier ident, StructType schema, Transform[] transforms, Map<String, String> properties)
      throws TableAlreadyExistsException {
    rejectPaimonStagedOperation(properties);
    return this.icebergsSparkCatalog.stageCreate(ident, schema, transforms, properties);
  }

  @Override
  @SuppressWarnings({"deprecation", "RedundantSuppression"})
  public StagedTable stageReplace(
      Identifier ident, StructType schema, Transform[] transforms, Map<String, String> properties)
      throws NoSuchTableException {
    rejectPaimonStagedOperation(properties);
    return this.icebergsSparkCatalog.stageReplace(ident, schema, transforms, properties);
  }

  @Override
  @SuppressWarnings({"deprecation", "RedundantSuppression"})
  public StagedTable stageCreateOrReplace(
      Identifier ident, StructType schema, Transform[] transforms, Map<String, String> properties) {
    rejectPaimonStagedOperation(properties);
    return this.icebergsSparkCatalog.stageCreateOrReplace(ident, schema, transforms, properties);
  }

  private static void rejectPaimonStagedOperation(Map<String, String> properties) {
    String provider = properties.get(PolarisCatalogUtils.TABLE_PROVIDER_KEY);
    if (PolarisCatalogUtils.usePaimon(provider)) {
      throw new UnsupportedOperationException(
          "Staged Paimon table operations, including CTAS and RTAS, are not supported");
    }
  }

  @Override
  public String[] defaultNamespace() {
    return this.icebergsSparkCatalog.defaultNamespace();
  }

  @Override
  public String[][] listNamespaces() {
    return this.icebergsSparkCatalog.listNamespaces();
  }

  @Override
  public String[][] listNamespaces(String[] namespace) throws NoSuchNamespaceException {
    return this.icebergsSparkCatalog.listNamespaces(namespace);
  }

  @Override
  public Map<String, String> loadNamespaceMetadata(String[] namespace)
      throws NoSuchNamespaceException {
    return this.icebergsSparkCatalog.loadNamespaceMetadata(namespace);
  }

  @Override
  public void createNamespace(String[] namespace, Map<String, String> metadata)
      throws NamespaceAlreadyExistsException {
    this.icebergsSparkCatalog.createNamespace(namespace, metadata);
  }

  @Override
  public void alterNamespace(String[] namespace, NamespaceChange... changes)
      throws NoSuchNamespaceException {
    this.icebergsSparkCatalog.alterNamespace(namespace, changes);
  }

  @Override
  public boolean dropNamespace(String[] namespace, boolean cascade)
      throws NoSuchNamespaceException {
    return this.icebergsSparkCatalog.dropNamespace(namespace, cascade);
  }

  public Identifier[] listViews(String... namespace) {
    return this.icebergsSparkCatalog.listViews(namespace);
  }

  public View loadView(Identifier ident) throws NoSuchViewException {
    return this.icebergsSparkCatalog.loadView(ident);
  }

  public View alterView(Identifier ident, ViewChange... changes)
      throws NoSuchViewException, IllegalArgumentException {
    return this.icebergsSparkCatalog.alterView(ident, changes);
  }

  public boolean dropView(Identifier ident) {
    return this.icebergsSparkCatalog.dropView(ident);
  }

  public void renameView(Identifier fromIdentifier, Identifier toIdentifier)
      throws NoSuchViewException, ViewAlreadyExistsException {
    this.icebergsSparkCatalog.renameView(fromIdentifier, toIdentifier);
  }

  public View replaceView(
      Identifier ident,
      String sql,
      String currentCatalog,
      String[] currentNamespace,
      StructType schema,
      String[] queryColumnNames,
      String[] columnAliases,
      String[] columnComments,
      Map<String, String> properties)
      throws NoSuchNamespaceException, NoSuchViewException {
    return this.icebergsSparkCatalog.replaceView(
        ident,
        sql,
        currentCatalog,
        currentNamespace,
        schema,
        queryColumnNames,
        columnAliases,
        columnComments,
        properties);
  }
}
