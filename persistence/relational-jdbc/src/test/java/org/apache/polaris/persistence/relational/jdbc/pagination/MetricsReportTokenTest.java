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

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;
import org.apache.polaris.core.persistence.metrics.MetricsRecordIdentity;
import org.apache.polaris.core.persistence.pagination.Page;
import org.apache.polaris.core.persistence.pagination.PageToken;
import org.junit.jupiter.api.Test;

class MetricsReportTokenTest {

  @Test
  void tokenIdIsM() {
    assertThat(MetricsReportToken.ID).isEqualTo("m");
    MetricsReportToken token =
        ImmutableMetricsReportToken.builder().timestampMs(1000L).reportId("abc").build();
    assertThat(token.getT()).isEqualTo("m");
  }

  @Test
  void tokenTypeRegistration() {
    // TokenType must be reachable via ServiceLoader for deserialization to work.
    MetricsReportToken.MetricsReportTokenType type =
        new MetricsReportToken.MetricsReportTokenType();
    assertThat(type.id()).isEqualTo(MetricsReportToken.ID);
    assertThat(type.javaType()).isEqualTo(MetricsReportToken.class);
  }

  @Test
  void fromRecordNull() {
    assertThat(MetricsReportToken.fromRecord(null)).isNull();
  }

  @Test
  void fromRecordBuildsToken() {
    MetricsRecordIdentity identity = stubIdentity("report-1", 5000L);
    MetricsReportToken token = MetricsReportToken.fromRecord(identity);

    assertThat(token).isNotNull();
    assertThat(token.reportId()).isEqualTo("report-1");
    assertThat(token.timestampMs()).isEqualTo(5000L);
  }

  @Test
  void serializeRoundTrip() {
    // Build a PageToken carrying a MetricsReportToken, encode it, then decode.
    MetricsRecordIdentity seed = stubIdentity("round-trip-id", 9_000_000L);
    MetricsReportToken cursor = MetricsReportToken.fromRecord(seed);
    assertThat(cursor).isNotNull();

    // Produce a Page with one item so we get an encodedResponseToken back.
    PageToken firstPage = PageToken.fromLimit(1);
    Page<String> page =
        Page.mapped(
            firstPage, Stream.of("item-a", "item-b"), Function.identity(), ignored -> cursor);

    String encoded = page.encodedResponseToken();
    assertThat(encoded).isNotNull();

    // Decode as a new PageToken and extract cursor.
    PageToken decoded = PageToken.build(encoded, 1, () -> true);
    assertThat(decoded.paginationRequested()).isTrue();
    assertThat(decoded.valueAs(MetricsReportToken.class)).isPresent();

    MetricsReportToken recovered = decoded.valueAs(MetricsReportToken.class).get();
    assertThat(recovered.reportId()).isEqualTo("round-trip-id");
    assertThat(recovered.timestampMs()).isEqualTo(9_000_000L);
  }

  @Test
  void paginationWithCursorStopsAtPageSize() {
    PageToken request = PageToken.fromLimit(2);
    List<String> items = List.of("a", "b", "c", "d");
    MetricsRecordIdentity identity = stubIdentity("id-b", 2000L);

    Page<String> page =
        Page.mapped(
            request,
            items.stream(),
            Function.identity(),
            item -> item.equals("b") ? MetricsReportToken.fromRecord(identity) : null);

    assertThat(page.items()).hasSize(2).containsExactly("a", "b");
    assertThat(page.encodedResponseToken()).isNotNull();
  }

  @Test
  void lastPageReturnsNullToken() {
    PageToken request = PageToken.fromLimit(10);
    Page<String> page =
        Page.mapped(
            request,
            Stream.of("x", "y"),
            Function.identity(),
            // tokenBuilder receives null when there is no next page; must return null in that case.
            item -> item != null ? MetricsReportToken.fromRecord(stubIdentity("id", 1L)) : null);

    // Fewer items than page size — no next page.
    assertThat(page.items()).hasSize(2);
    assertThat(page.encodedResponseToken()).isNull();
  }

  // Minimal stub so we don't need a full ScanMetricsRecord/CommitMetricsRecord.
  private static MetricsRecordIdentity stubIdentity(String reportId, long timestampMs) {
    return new MetricsRecordIdentity() {
      @Override
      public String reportId() {
        return reportId;
      }

      @Override
      public long catalogId() {
        return 0;
      }

      @Override
      public long tableId() {
        return 0;
      }

      @Override
      public Instant timestamp() {
        return Instant.ofEpochMilli(timestampMs);
      }

      @Override
      public java.util.Map<String, String> metadata() {
        return java.util.Map.of();
      }

      @Override
      public String principalName() {
        return null;
      }

      @Override
      public String requestId() {
        return null;
      }

      @Override
      public String otelTraceId() {
        return null;
      }

      @Override
      public String otelSpanId() {
        return null;
      }
    };
  }
}
