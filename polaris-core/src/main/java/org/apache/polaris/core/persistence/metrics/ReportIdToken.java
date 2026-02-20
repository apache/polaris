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
import com.google.common.annotations.Beta;
import jakarta.annotation.Nullable;
import org.apache.polaris.core.persistence.pagination.Token;
import org.apache.polaris.immutables.PolarisImmutable;

/**
 * Pagination {@linkplain Token token} for metrics queries, using a composite cursor of timestamp
 * and report ID.
 *
 * <p><strong>Note:</strong> This is a reference implementation provided for convenience. It is
 * <em>not required</em> by the {@link MetricsPersistence} SPI contract. Persistence backends are
 * free to implement their own {@link Token} subclass optimized for their storage model (e.g.,
 * timestamp-based cursors, composite keys, continuation tokens).
 *
 * <p>Only {@link org.apache.polaris.core.persistence.pagination.PageToken} (for requests) and
 * {@link org.apache.polaris.core.persistence.pagination.Page} (for responses) are required by the
 * SPI contract.
 *
 * <p>This token enables cursor-based pagination for metrics queries across different storage
 * backends. It uses a composite cursor of (timestamp_ms, report_id) to provide:
 *
 * <ul>
 *   <li>Chronological ordering - results are returned in timestamp order
 *   <li>Deterministic pagination - report_id breaks ties for same-millisecond entries
 *   <li>Efficient queries - can use compound indexes on (timestamp_ms, report_id)
 * </ul>
 *
 * <p>Each backend implementation can use this cursor value to implement efficient pagination:
 *
 * <ul>
 *   <li>RDBMS: {@code WHERE (timestamp_ms, report_id) > (:ts, :id) ORDER BY timestamp_ms,
 *       report_id}
 *   <li>NoSQL: Use timestamp as partition key and report_id as sort key
 * </ul>
 *
 * <p><b>Note:</b> This type is part of the experimental Metrics Persistence SPI and may change in
 * future releases.
 */
@Beta
@PolarisImmutable
@JsonSerialize(as = ImmutableReportIdToken.class)
@JsonDeserialize(as = ImmutableReportIdToken.class)
public interface ReportIdToken extends Token {

  /** Token type identifier. Short to minimize serialized token size. */
  String ID = "r";

  /**
   * The report ID to use as the cursor (tie-breaker for same-timestamp entries).
   *
   * <p>Results should start after this report ID within the same timestamp. This is typically the
   * {@code reportId} of the last item from the previous page.
   */
  @JsonProperty("r")
  String reportId();

  /**
   * The timestamp in milliseconds to use as the primary cursor.
   *
   * <p>Results should start at or after this timestamp. Combined with {@link #reportId()}, this
   * provides a composite cursor for deterministic chronological pagination.
   */
  @JsonProperty("ts")
  long timestampMs();

  @Override
  default String getT() {
    return ID;
  }

  /**
   * Creates a token from a report ID and timestamp.
   *
   * @param reportId the report ID to use as cursor
   * @param timestampMs the timestamp in milliseconds
   * @return the token, or null if reportId is null
   */
  static @Nullable ReportIdToken from(@Nullable String reportId, long timestampMs) {
    if (reportId == null) {
      return null;
    }
    return ImmutableReportIdToken.builder().reportId(reportId).timestampMs(timestampMs).build();
  }

  /**
   * Creates a token from a scan metrics record.
   *
   * @param record the record whose report ID and timestamp should be used as cursor
   * @return the token, or null if record is null
   */
  static @Nullable ReportIdToken fromRecord(@Nullable ScanMetricsRecord record) {
    if (record == null) {
      return null;
    }
    return from(record.reportId(), record.timestamp().toEpochMilli());
  }

  /**
   * Creates a token from a commit metrics record.
   *
   * @param record the record whose report ID and timestamp should be used as cursor
   * @return the token, or null if record is null
   */
  static @Nullable ReportIdToken fromRecord(@Nullable CommitMetricsRecord record) {
    if (record == null) {
      return null;
    }
    return from(record.reportId(), record.timestamp().toEpochMilli());
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
