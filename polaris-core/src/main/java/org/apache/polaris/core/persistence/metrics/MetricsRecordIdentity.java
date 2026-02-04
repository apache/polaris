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
import java.util.List;
import java.util.Map;

/**
 * Base interface containing common identification fields shared by all metrics records.
 *
 * <p>This interface defines the common fields that identify the source of a metrics report,
 * including the report ID, catalog ID, table location, timestamp, and metadata.
 *
 * <p>Both {@link ScanMetricsRecord} and {@link CommitMetricsRecord} extend this interface to
 * inherit these common fields while adding their own specific metrics.
 *
 * <h3>Design Decisions</h3>
 *
 * <p><b>Entity IDs only (no names):</b> We store only catalog ID and table ID, not their names.
 * Names can change over time (via rename operations), which would make querying historical metrics
 * by name challenging and lead to correctness issues. Queries should resolve names to IDs using the
 * current catalog state.
 *
 * <p><b>Namespace as List&lt;String&gt;:</b> Namespaces are stored as a list of levels rather than
 * a dot-separated string to avoid ambiguity when namespace segments contain dots. The persistence
 * implementation handles the serialization format.
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
   * PolarisEntityCore#getId()}. The catalog name is not stored since it can change over time;
   * queries should resolve names to IDs using the current catalog state.
   */
  long catalogId();

  /**
   * Namespace path as a list of levels (e.g., ["db", "schema"]).
   *
   * <p>This is the namespace portion of the table identifier. Using a list avoids ambiguity when
   * namespace segments contain dots. The persistence implementation handles the serialization
   * format.
   */
  List<String> namespace();

  /**
   * Internal table entity ID.
   *
   * <p>This matches the table entity ID in Polaris persistence, as defined by {@code
   * PolarisEntityCore#getId()}. The table name is not stored since it can change over time; queries
   * should resolve names to IDs using the current catalog state.
   */
  long tableId();

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
