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

/**
 * Base interface containing common identification fields shared by all metrics records.
 *
 * <p>This interface defines the common fields that identify the source of a metrics report,
 * including the report ID, catalog information, namespace, table name, timestamp, and metadata.
 *
 * <p>Both {@link ScanMetricsRecord} and {@link CommitMetricsRecord} extend this interface to
 * inherit these common fields while adding their own specific metrics.
 *
 * <p>Note: Realm ID is intentionally not included in this interface. Multi-tenancy realm context
 * should be obtained from the CDI-injected {@code RealmContext} at persistence time. This keeps
 * catalog-specific code from needing to manage realm concerns.
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
   * Dot-separated namespace path (e.g., "db.schema").
   *
   * <p>The namespace containing the table for which metrics are reported.
   */
  String namespace();

  /**
   * Table name.
   *
   * <p>The name of the table for which metrics are reported.
   */
  String tableName();

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
