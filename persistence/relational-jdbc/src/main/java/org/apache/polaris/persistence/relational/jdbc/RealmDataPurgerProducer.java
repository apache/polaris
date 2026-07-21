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

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import org.apache.polaris.persistence.relational.jdbc.models.ModelEvent;

/**
 * Produces a {@link RealmDataPurger} for each realm-scoped time-series table defined in this
 * module's schema (see {@code schema-v*.sql}). The drainer discovers them via {@code
 * Instance<RealmDataPurger>}, so adding a table means adding a producer here, not editing the
 * drainer.
 *
 * <p>The metrics report tables' write path moved to the metrics extension (Apache Polaris #5068),
 * but the tables themselves remain in this schema, so purging them stays a concern of this module.
 */
@ApplicationScoped
public class RealmDataPurgerProducer {

  // Time-series tables owned by this module's schema. events has a ModelEvent constant; the metrics
  // report tables no longer have a Model class here (moved in #5068), so they are named directly.
  static final String SCAN_METRICS_REPORT = "scan_metrics_report";
  static final String COMMIT_METRICS_REPORT = "commit_metrics_report";

  @Produces
  @ApplicationScoped
  RealmDataPurger eventsPurger(DatasourceOperations ops) {
    return new TableRealmDataPurger(ops, ModelEvent.TABLE_NAME, "event_id");
  }

  @Produces
  @ApplicationScoped
  RealmDataPurger scanMetricsReportPurger(DatasourceOperations ops) {
    return new TableRealmDataPurger(ops, SCAN_METRICS_REPORT, "report_id");
  }

  @Produces
  @ApplicationScoped
  RealmDataPurger commitMetricsReportPurger(DatasourceOperations ops) {
    return new TableRealmDataPurger(ops, COMMIT_METRICS_REPORT, "report_id");
  }
}
