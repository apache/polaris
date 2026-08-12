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

import io.quarkus.arc.DefaultBean;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.List;
import org.apache.polaris.core.persistence.metrics.MetricsRecordIdentity;
import org.apache.polaris.core.persistence.pagination.Page;
import org.apache.polaris.core.persistence.pagination.PageToken;
import org.apache.polaris.extension.metrics.spi.MetricsQuerySpi;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/**
 * No-op default implementation of {@link MetricsQuerySpi} that returns empty pages.
 *
 * <p>Annotated {@link DefaultBean} so it is only active when no durable query backend (e.g. the
 * {@code polaris-extensions-metrics-reports-jdbc} extension) contributes a {@link MetricsQuerySpi}.
 * With this default present the read API always resolves a provider: it returns an empty result set
 * until a durable backend is installed.
 */
@ApplicationScoped
@DefaultBean
public class NoOpMetricsQuery implements MetricsQuerySpi {

  @Override
  public Page<? extends MetricsRecordIdentity> listReports(
      @NonNull MetricType metricType,
      long catalogId,
      long tableId,
      @Nullable Long snapshotId,
      @Nullable String principalName,
      @Nullable Long timestampFrom,
      @Nullable Long timestampTo,
      @NonNull PageToken pageToken) {
    return Page.fromItems(List.of());
  }
}
