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
package org.apache.polaris.persistence.relational.jdbc.pagination;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import jakarta.annotation.Nullable;
import org.apache.polaris.core.persistence.metrics.MetricsRecordIdentity;
import org.apache.polaris.core.persistence.pagination.Token;
import org.apache.polaris.immutables.PolarisImmutable;

/**
 * Pagination {@linkplain Token token} for metrics reports, backed by {@code (timestamp_ms,
 * report_id)}.
 *
 * <p>Metrics reports are sorted by {@code (timestamp_ms DESC, report_id DESC)}. The cursor encodes
 * the last-seen {@code (timestamp_ms, report_id)} pair, enabling stable keyset pagination that
 * remains correct under concurrent inserts.
 */
@PolarisImmutable
@JsonSerialize(as = ImmutableMetricsReportToken.class)
@JsonDeserialize(as = ImmutableMetricsReportToken.class)
public interface MetricsReportToken extends Token {
  // Registered token type IDs (must be unique across all Token implementations):
  //   "e" - EntityIdToken
  //   "m" - MetricsReportToken  (this class)
  String ID = "m";

  @JsonProperty("ts")
  long timestampMs();

  @JsonProperty("id")
  String reportId();

  @Override
  default String getT() {
    return ID;
  }

  static @Nullable MetricsReportToken fromRecord(@Nullable MetricsRecordIdentity record) {
    if (record == null) {
      return null;
    }
    return ImmutableMetricsReportToken.builder()
        .timestampMs(record.timestamp().toEpochMilli())
        .reportId(record.reportId())
        .build();
  }

  final class MetricsReportTokenType implements TokenType {
    @Override
    public String id() {
      return ID;
    }

    @Override
    public Class<? extends Token> javaType() {
      return MetricsReportToken.class;
    }
  }
}
