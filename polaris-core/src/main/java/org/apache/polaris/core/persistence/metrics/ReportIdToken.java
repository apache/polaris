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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import jakarta.annotation.Nullable;
import org.apache.polaris.core.persistence.pagination.Token;
import org.apache.polaris.immutables.PolarisImmutable;

/**
 * Pagination {@linkplain Token token} for metrics queries, backed by the report ID (UUID).
 *
 * <p>This token enables cursor-based pagination for metrics queries across different storage
 * backends. The report ID is used as the cursor because it is:
 *
 * <ul>
 *   <li>Guaranteed unique across all reports
 *   <li>Present in both scan and commit metrics records
 *   <li>Stable (doesn't change over time)
 * </ul>
 *
 * <p>Each backend implementation can use this cursor value to implement efficient pagination in
 * whatever way is optimal for that storage system:
 *
 * <ul>
 *   <li>RDBMS: {@code WHERE report_id > :lastReportId ORDER BY report_id}
 *   <li>NoSQL: Use report ID as partition/sort key cursor
 *   <li>Time-series: Combine with timestamp for efficient range scans
 * </ul>
 */
@PolarisImmutable
@JsonSerialize(as = ImmutableReportIdToken.class)
@JsonDeserialize(as = ImmutableReportIdToken.class)
public interface ReportIdToken extends Token {

  /** Token type identifier. Short to minimize serialized token size. */
  String ID = "r";

  /**
   * The report ID to use as the cursor.
   *
   * <p>Results should start after this report ID. This is typically the {@code reportId} of the
   * last item from the previous page.
   */
  @JsonProperty("r")
  String reportId();

  @Override
  default String getT() {
    return ID;
  }

  /**
   * Creates a token from a report ID.
   *
   * @param reportId the report ID to use as cursor
   * @return the token, or null if reportId is null
   */
  static @Nullable ReportIdToken fromReportId(@Nullable String reportId) {
    if (reportId == null) {
      return null;
    }
    return ImmutableReportIdToken.builder().reportId(reportId).build();
  }

  /**
   * Creates a token from a metrics record.
   *
   * @param record the record whose report ID should be used as cursor
   * @return the token, or null if record is null
   */
  static @Nullable ReportIdToken fromRecord(@Nullable ScanMetricsRecord record) {
    if (record == null) {
      return null;
    }
    return fromReportId(record.reportId());
  }

  /**
   * Creates a token from a commit metrics record.
   *
   * @param record the record whose report ID should be used as cursor
   * @return the token, or null if record is null
   */
  static @Nullable ReportIdToken fromRecord(@Nullable CommitMetricsRecord record) {
    if (record == null) {
      return null;
    }
    return fromReportId(record.reportId());
  }

  /** Token type registration for service loader. */
  final class ReportIdTokenType implements TokenType {
    @Override
    public String id() {
      return ID;
    }

    @Override
    public Class<? extends Token> javaType() {
      return ReportIdToken.class;
    }
  }
}
