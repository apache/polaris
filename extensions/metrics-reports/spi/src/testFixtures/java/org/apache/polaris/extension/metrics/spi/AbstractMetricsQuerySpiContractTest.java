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
package org.apache.polaris.extension.metrics.spi;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.apache.polaris.core.persistence.metrics.CommitMetricsRecord;
import org.apache.polaris.core.persistence.metrics.MetricsRecordIdentity;
import org.apache.polaris.core.persistence.metrics.ScanMetricsRecord;
import org.apache.polaris.core.persistence.pagination.EntityIdToken;
import org.apache.polaris.core.persistence.pagination.ImmutablePageToken;
import org.apache.polaris.core.persistence.pagination.Page;
import org.apache.polaris.core.persistence.pagination.PageToken;
import org.junit.jupiter.api.Test;

/**
 * Contract test shared by every {@link MetricsQuerySpi} implementation (the no-op default and any
 * durable backend, e.g. the JDBC extension), so they're all held to the same read-path behavior.
 *
 * <p>Implementations that don't actually persist data (e.g. the no-op default) can't satisfy the
 * pagination-specific cases below, since there's never anything to page through or a cursor to
 * validate; those cases are skipped via {@link #supportsPersistence()} rather than duplicated.
 */
public abstract class AbstractMetricsQuerySpiContractTest {

  private static final long CATALOG_ID = 100L;
  private static final long TABLE_ID = 200L;

  /** Returns the {@link MetricsQuerySpi} under test, scoped to the given realm. */
  protected abstract MetricsQuerySpi querySpi(String realmId);

  /** Writes a scan report visible to readers of {@code realmId}, if the backend persists data. */
  protected abstract void writeScan(String realmId, ScanMetricsRecord record);

  /** Writes a commit report visible to readers of {@code realmId}, if the backend persists data. */
  protected abstract void writeCommit(String realmId, CommitMetricsRecord record);

  /**
   * Whether this backend actually stores and returns rows. {@code false} for the no-op default,
   * which always returns empty pages regardless of what's "written".
   */
  protected boolean supportsPersistence() {
    return true;
  }

  @Test
  void emptyResultReturnsEmptyPage() {
    MetricsQuerySpi spi = querySpi(realm());
    Page<? extends MetricsRecordIdentity> scanPage =
        spi.listReports(
            MetricsQuerySpi.MetricType.SCAN,
            CATALOG_ID,
            List.of(TABLE_ID),
            null,
            null,
            null,
            PageToken.fromLimit(10));
    Page<? extends MetricsRecordIdentity> commitPage =
        spi.listReports(
            MetricsQuerySpi.MetricType.COMMIT,
            CATALOG_ID,
            List.of(TABLE_ID),
            null,
            null,
            null,
            PageToken.fromLimit(10));

    assertThat(scanPage.items()).isEmpty();
    assertThat(commitPage.items()).isEmpty();
  }

  @Test
  void realmIsolationForIdenticalCatalogAndTableIds() {
    String realmA = realm();
    String realmB = realm();

    ScanMetricsRecord record =
        scanRecord(UUID.randomUUID().toString(), CATALOG_ID, TABLE_ID, Instant.now());
    writeScan(realmA, record);

    Page<? extends MetricsRecordIdentity> otherRealmPage =
        querySpi(realmB)
            .listReports(
                MetricsQuerySpi.MetricType.SCAN,
                CATALOG_ID,
                List.of(TABLE_ID),
                null,
                null,
                null,
                PageToken.fromLimit(10));
    assertThat(otherRealmPage.items()).isEmpty();

    if (supportsPersistence()) {
      Page<? extends MetricsRecordIdentity> ownRealmPage =
          querySpi(realmA)
              .listReports(
                  MetricsQuerySpi.MetricType.SCAN,
                  CATALOG_ID,
                  List.of(TABLE_ID),
                  null,
                  null,
                  null,
                  PageToken.fromLimit(10));
      assertThat(ownRealmPage.items())
          .extracting(MetricsRecordIdentity::reportId)
          .contains(record.reportId());
    }
  }

  @Test
  void twoPagesPaginationWorks() {
    assumeTrue(supportsPersistence(), "backend does not persist data");
    String realm = realm();
    Instant base = Instant.now();
    ScanMetricsRecord oldest =
        scanRecord(UUID.randomUUID().toString(), CATALOG_ID, TABLE_ID, base.minusSeconds(2));
    ScanMetricsRecord middle =
        scanRecord(UUID.randomUUID().toString(), CATALOG_ID, TABLE_ID, base.minusSeconds(1));
    ScanMetricsRecord newest = scanRecord(UUID.randomUUID().toString(), CATALOG_ID, TABLE_ID, base);
    writeScan(realm, oldest);
    writeScan(realm, middle);
    writeScan(realm, newest);

    Page<? extends MetricsRecordIdentity> firstPage =
        querySpi(realm)
            .listReports(
                MetricsQuerySpi.MetricType.SCAN,
                CATALOG_ID,
                List.of(TABLE_ID),
                null,
                null,
                null,
                PageToken.fromLimit(2));
    assertThat(firstPage.items())
        .extracting(MetricsRecordIdentity::reportId)
        .containsExactly(newest.reportId(), middle.reportId());
    assertThat(firstPage.encodedResponseToken()).isNotNull();

    PageToken nextPageToken = PageToken.build(firstPage.encodedResponseToken(), 2, () -> true);
    Page<? extends MetricsRecordIdentity> secondPage =
        querySpi(realm)
            .listReports(
                MetricsQuerySpi.MetricType.SCAN,
                CATALOG_ID,
                List.of(TABLE_ID),
                null,
                null,
                null,
                nextPageToken);
    assertThat(secondPage.items())
        .extracting(MetricsRecordIdentity::reportId)
        .containsExactly(oldest.reportId());
    assertThat(secondPage.encodedResponseToken()).isNull();
  }

  @Test
  void tieOnEqualTimestampBreaksByReportId() {
    assumeTrue(supportsPersistence(), "backend does not persist data");
    String realm = realm();
    Instant sameTimestamp = Instant.now();
    ScanMetricsRecord first =
        scanRecord("aaaaaaaa-0000-0000-0000-000000000000", CATALOG_ID, TABLE_ID, sameTimestamp);
    ScanMetricsRecord second =
        scanRecord("bbbbbbbb-0000-0000-0000-000000000000", CATALOG_ID, TABLE_ID, sameTimestamp);
    writeScan(realm, first);
    writeScan(realm, second);

    Page<? extends MetricsRecordIdentity> firstPage =
        querySpi(realm)
            .listReports(
                MetricsQuerySpi.MetricType.SCAN,
                CATALOG_ID,
                List.of(TABLE_ID),
                null,
                null,
                null,
                PageToken.fromLimit(1));
    assertThat(firstPage.items())
        .extracting(MetricsRecordIdentity::reportId)
        .containsExactly(second.reportId());

    PageToken nextPageToken = PageToken.build(firstPage.encodedResponseToken(), 1, () -> true);
    Page<? extends MetricsRecordIdentity> secondPage =
        querySpi(realm)
            .listReports(
                MetricsQuerySpi.MetricType.SCAN,
                CATALOG_ID,
                List.of(TABLE_ID),
                null,
                null,
                null,
                nextPageToken);
    assertThat(secondPage.items())
        .extracting(MetricsRecordIdentity::reportId)
        .containsExactly(first.reportId());
    assertThat(secondPage.encodedResponseToken()).isNull();
  }

  @Test
  void foreignCursorTokenTypeIsRejected() {
    assumeTrue(supportsPersistence(), "backend does not validate cursor types");
    // No page size requested, only a cursor value from an unrelated token type: the wrong-type
    // guard must fire regardless of whether pagination was explicitly requested.
    PageToken foreignCursor =
        ImmutablePageToken.builder().value(Optional.of(EntityIdToken.fromEntityId(1L))).build();

    assertThatThrownBy(
            () ->
                querySpi(realm())
                    .listReports(
                        MetricsQuerySpi.MetricType.SCAN,
                        CATALOG_ID,
                        List.of(TABLE_ID),
                        null,
                        null,
                        null,
                        foreignCursor))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void crossRealmCursorReplayIsRejected() {
    assumeTrue(supportsPersistence(), "backend does not persist data");
    String realmA = realm();
    String realmB = realm();

    Instant base = Instant.now();
    writeScan(realmA, scanRecord(UUID.randomUUID().toString(), CATALOG_ID, TABLE_ID, base));
    writeScan(
        realmA,
        scanRecord(UUID.randomUUID().toString(), CATALOG_ID, TABLE_ID, base.minusSeconds(1)));
    ScanMetricsRecord recordB =
        scanRecord(UUID.randomUUID().toString(), CATALOG_ID, TABLE_ID, Instant.now());
    writeScan(realmB, recordB);

    Page<? extends MetricsRecordIdentity> firstPage =
        querySpi(realmA)
            .listReports(
                MetricsQuerySpi.MetricType.SCAN,
                CATALOG_ID,
                List.of(TABLE_ID),
                null,
                null,
                null,
                PageToken.fromLimit(1));
    assertThat(firstPage.encodedResponseToken()).isNotNull();

    // A cursor minted while listing realm A must not be honored when replayed against realm B,
    // even though catalog/table ids are identical: otherwise realm B's query would apply realm
    // A's keyset predicate to realm B's result set.
    PageToken replayedToken = PageToken.build(firstPage.encodedResponseToken(), 1, () -> true);
    assertThatThrownBy(
            () ->
                querySpi(realmB)
                    .listReports(
                        MetricsQuerySpi.MetricType.SCAN,
                        CATALOG_ID,
                        List.of(TABLE_ID),
                        null,
                        null,
                        null,
                        replayedToken))
        .isInstanceOf(IllegalArgumentException.class);
  }

  private static String realm() {
    return "realm-" + UUID.randomUUID();
  }

  protected static ScanMetricsRecord scanRecord(
      String reportId, long catalogId, long tableId, Instant timestamp) {
    return ScanMetricsRecord.builder()
        .reportId(reportId)
        .catalogId(catalogId)
        .tableId(tableId)
        .timestamp(timestamp)
        .resultDataFiles(0L)
        .resultDeleteFiles(0L)
        .totalFileSizeBytes(0L)
        .totalDataManifests(0L)
        .totalDeleteManifests(0L)
        .scannedDataManifests(0L)
        .scannedDeleteManifests(0L)
        .skippedDataManifests(0L)
        .skippedDeleteManifests(0L)
        .skippedDataFiles(0L)
        .skippedDeleteFiles(0L)
        .totalPlanningDurationMs(0L)
        .equalityDeleteFiles(0L)
        .positionalDeleteFiles(0L)
        .indexedDeleteFiles(0L)
        .totalDeleteFileSizeBytes(0L)
        .build();
  }

  protected static CommitMetricsRecord commitRecord(
      String reportId, long catalogId, long tableId, Instant timestamp) {
    return CommitMetricsRecord.builder()
        .reportId(reportId)
        .catalogId(catalogId)
        .tableId(tableId)
        .timestamp(timestamp)
        .snapshotId(1L)
        .operation("append")
        .addedDataFiles(0L)
        .removedDataFiles(0L)
        .totalDataFiles(0L)
        .addedDeleteFiles(0L)
        .removedDeleteFiles(0L)
        .totalDeleteFiles(0L)
        .addedEqualityDeleteFiles(0L)
        .removedEqualityDeleteFiles(0L)
        .addedPositionalDeleteFiles(0L)
        .removedPositionalDeleteFiles(0L)
        .addedRecords(0L)
        .removedRecords(0L)
        .totalRecords(0L)
        .addedFileSizeBytes(0L)
        .removedFileSizeBytes(0L)
        .totalFileSizeBytes(0L)
        .attempts(0)
        .build();
  }
}
