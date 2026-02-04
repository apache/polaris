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
package org.apache.polaris.core.persistence.metrics;

import java.time.Instant;
import java.util.Map;
import org.apache.iceberg.catalog.TableIdentifier;

/**
 * Base interface containing common identification fields shared by all metrics records.
 *
 * <p>This interface defines the common fields that identify the source of a metrics report,
 * including the report ID, catalog information, table identifier, timestamp, and metadata.
 *
 * <p>Both {@link ScanMetricsRecord} and {@link CommitMetricsRecord} extend this interface to
 * inherit these common fields while adding their own specific metrics.
 *
 * <h3>Design Decisions</h3>
 *
 * <p><b>TableIdentifier vs separate namespace/tableName:</b> We use Iceberg's {@link
 * TableIdentifier} which encapsulates both namespace and table name. This aligns with how Iceberg
 * reports identify tables and is consistent with Polaris entity patterns (e.g., {@code
 * TableLikeEntity.getTableIdentifier()}).
 *
 * <p><b>Catalog ID/Name vs CatalogEntity:</b> We use separate primitive fields for catalog ID and
 * name rather than {@code CatalogEntity} because:
 *
 * <ul>
 *   <li>{@code CatalogEntity} is a heavyweight object containing storage config, properties, and
 *       other data not relevant for metrics identification
 *   <li>{@code CatalogEntity} is not an Immutables-compatible interface, making it difficult to
 *       include in {@code @PolarisImmutable} generated classes
 *   <li>For metrics, we only need the catalog ID (for foreign key relationships) and name (for
 *       display/query convenience)
 * </ul>
 *
 * <p><b>Realm ID:</b> Realm ID is intentionally not included in this interface. Multi-tenancy realm
 * context should be obtained from the CDI-injected {@code RealmContext} at persistence time. This
 * keeps catalog-specific code from needing to manage realm concerns.
 */
public interface MetricsRecordIdentity {

  /**
   * Unique identifier for this report (UUID).
   *
   * <p>This ID is generated when the record is created and serves as the primary key for the
   * metrics record in persistence storage.
   */
  String reportId();

  /**
   * Internal catalog ID.
   *
   * <p>This matches the catalog entity ID in Polaris persistence, as defined by {@code
   * PolarisEntityCore#getId()}.
   */
  long catalogId();

  /**
   * Human-readable catalog name.
   *
   * <p>The catalog name as known to clients. This is stored alongside the ID for query convenience
   * and display purposes.
   */
  String catalogName();

  /**
   * Table identifier including namespace and table name.
   *
   * <p>This uses Iceberg's {@link TableIdentifier} which encapsulates both the namespace path and
   * the table name. The namespace can be accessed via {@link TableIdentifier#namespace()} and the
   * table name via {@link TableIdentifier#name()}.
   *
   * <p>Example: For a table "my_table" in namespace "db.schema", use {@code
   * TableIdentifier.of(Namespace.of("db", "schema"), "my_table")}.
   */
  TableIdentifier tableIdentifier();

  /**
   * Timestamp when the report was received.
   *
   * <p>This is the server-side timestamp when the metrics report was processed, not the client-side
   * timestamp when the operation occurred.
   */
  Instant timestamp();

  /**
   * Additional metadata as key-value pairs.
   *
   * <p>This map can contain additional contextual information from the original Iceberg report,
   * including client-provided trace IDs or other correlation data. Persistence implementations can
   * store and index specific metadata fields as needed.
   */
  Map<String, String> metadata();
}
