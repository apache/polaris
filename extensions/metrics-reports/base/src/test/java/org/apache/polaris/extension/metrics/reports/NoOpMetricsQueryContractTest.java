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
package org.apache.polaris.extension.metrics.reports;

import org.apache.polaris.core.persistence.metrics.CommitMetricsRecord;
import org.apache.polaris.core.persistence.metrics.ScanMetricsRecord;
import org.apache.polaris.extension.metrics.spi.AbstractMetricsQuerySpiContractTest;
import org.apache.polaris.extension.metrics.spi.MetricsQuerySpi;

/**
 * Runs the shared {@link MetricsQuerySpi} contract against {@link NoOpMetricsQuery}. Persistence
 * cases are skipped (see {@link #supportsPersistence()}): there's nothing to page through or
 * isolate by realm when the backend never stores what it's given.
 */
class NoOpMetricsQueryContractTest extends AbstractMetricsQuerySpiContractTest {

  private final NoOpMetricsQuery noOpMetricsQuery = new NoOpMetricsQuery();

  @Override
  protected MetricsQuerySpi querySpi(String realmId) {
    return noOpMetricsQuery;
  }

  @Override
  protected void writeScan(String realmId, ScanMetricsRecord record) {
    // No-op: this backend never persists anything.
  }

  @Override
  protected void writeCommit(String realmId, CommitMetricsRecord record) {
    // No-op: this backend never persists anything.
  }

  @Override
  protected boolean supportsPersistence() {
    return false;
  }
}
