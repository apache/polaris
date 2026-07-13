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
package org.apache.polaris.persistence.relational.jdbc;

import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.polaris.core.persistence.bootstrap.BootstrapOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Bootstraps independently versioned JDBC schema components. */
final class JdbcSchemaManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(JdbcSchemaManager.class);

  private final DatasourceOperations datasourceOperations;

  JdbcSchemaManager(DatasourceOperations datasourceOperations) {
    this.datasourceOperations = datasourceOperations;
  }

  Map<JdbcSchemaComponent, Integer> bootstrap(BootstrapOptions bootstrapOptions) {
    boolean entityTableExists = JdbcBasePersistenceImpl.entityTableExists(datasourceOperations);
    Map<JdbcSchemaComponent, Integer> desiredVersions =
        desiredSchemaVersions(bootstrapOptions, entityTableExists);
    SchemaExecutionPlan plan = createExecutionPlan(desiredVersions);

    for (SchemaScript script : plan.scripts()) {
      LOGGER.info(
          "Bootstrapping JDBC schema component {} from version {} to version {}",
          script.component().versionKey(),
          script.currentVersion(),
          script.targetVersion());
      try {
        datasourceOperations.executeScript(
            datasourceOperations
                .getDatabaseType()
                .openInitScriptResource(script.component(), script.targetVersion()));
      } catch (SQLException e) {
        throw new RuntimeException(
            String.format(
                "Error executing %s schema script: %s",
                script.component().versionKey(), e.getMessage()),
            e);
      }
    }

    return plan.effectiveVersions();
  }

  private SchemaExecutionPlan createExecutionPlan(
      Map<JdbcSchemaComponent, Integer> desiredVersions) {
    Map<JdbcSchemaComponent, Integer> effectiveVersions = new LinkedHashMap<>();
    List<SchemaScript> scripts = new ArrayList<>();

    for (Map.Entry<JdbcSchemaComponent, Integer> entry : desiredVersions.entrySet()) {
      JdbcSchemaComponent component = entry.getKey();
      int targetVersion = entry.getValue();
      int currentVersion = loadComponentVersion(component);
      int effectiveVersion = effectiveComponentVersion(component, currentVersion, targetVersion);
      effectiveVersions.put(component, effectiveVersion);

      if (currentVersion != effectiveVersion) {
        validateScriptResource(component, effectiveVersion);
        scripts.add(new SchemaScript(component, currentVersion, effectiveVersion));
      }
    }

    return new SchemaExecutionPlan(Map.copyOf(effectiveVersions), List.copyOf(scripts));
  }

  private void validateScriptResource(JdbcSchemaComponent component, int schemaVersion) {
    try (InputStream ignored =
        datasourceOperations.getDatabaseType().openInitScriptResource(component, schemaVersion)) {
      // Open and close the resource during planning so invalid requests fail before SQL executes.
    } catch (IOException e) {
      throw new RuntimeException(
          String.format(
              "Error validating %s schema script version %d: %s",
              component.versionKey(), schemaVersion, e.getMessage()),
          e);
    }
  }

  private Map<JdbcSchemaComponent, Integer> desiredSchemaVersions(
      BootstrapOptions bootstrapOptions, boolean entityTableExists) {
    Map<JdbcSchemaComponent, Integer> desiredVersions = new LinkedHashMap<>();
    validateRequestedComponents(bootstrapOptions);
    int currentMetastoreVersion = loadComponentVersion(JdbcSchemaComponent.METASTORE);
    int requestedMetastoreVersion =
        JdbcBootstrapUtils.getRequestedSchemaVersion(
            bootstrapOptions, JdbcSchemaComponent.METASTORE);
    desiredVersions.put(
        JdbcSchemaComponent.METASTORE,
        JdbcBootstrapUtils.getRealmBootstrapSchemaVersion(
            datasourceOperations.getDatabaseType(),
            currentMetastoreVersion,
            requestedMetastoreVersion,
            entityTableExists));
    if (isComponentRequested(bootstrapOptions, JdbcSchemaComponent.LINEAGE)) {
      int requestedLineageVersion =
          JdbcBootstrapUtils.getRequestedSchemaVersion(
              bootstrapOptions, JdbcSchemaComponent.LINEAGE);
      desiredVersions.put(
          JdbcSchemaComponent.LINEAGE,
          requestedLineageVersion == -1
              ? JdbcSchemaComponent.LINEAGE.latestVersion(datasourceOperations.getDatabaseType())
              : requestedLineageVersion);
    }
    return desiredVersions;
  }

  private static void validateRequestedComponents(BootstrapOptions bootstrapOptions) {
    if (bootstrapOptions.schemaOptions() == null) {
      return;
    }
    for (String componentName :
        bootstrapOptions.schemaOptions().schemaComponentVersions().keySet()) {
      if (JdbcSchemaComponent.fromVersionKey(componentName).isEmpty()) {
        throw new IllegalArgumentException("Unknown JDBC schema component: " + componentName);
      }
    }
  }

  private static boolean isComponentRequested(
      BootstrapOptions bootstrapOptions, JdbcSchemaComponent component) {
    return bootstrapOptions.schemaOptions() != null
        && bootstrapOptions
            .schemaOptions()
            .schemaComponentVersions()
            .containsKey(component.versionKey());
  }

  private int loadComponentVersion(JdbcSchemaComponent component) {
    return JdbcBasePersistenceImpl.loadSchemaVersion(datasourceOperations, component, true);
  }

  private int effectiveComponentVersion(
      JdbcSchemaComponent component, int currentVersion, int targetVersion) {
    if (currentVersion == targetVersion) {
      return targetVersion;
    }
    if (currentVersion == 0) {
      return targetVersion;
    }
    throw new IllegalStateException(
        String.format(
            "Cannot upgrade JDBC schema component %s from version %d to version %d; component migrations are not implemented.",
            component.versionKey(), currentVersion, targetVersion));
  }

  private record SchemaExecutionPlan(
      Map<JdbcSchemaComponent, Integer> effectiveVersions, List<SchemaScript> scripts) {}

  private record SchemaScript(
      JdbcSchemaComponent component, int currentVersion, int targetVersion) {}
}
