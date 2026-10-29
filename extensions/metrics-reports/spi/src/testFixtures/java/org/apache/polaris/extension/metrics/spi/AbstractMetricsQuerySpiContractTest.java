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
import java.util.Set;
import java.util.UUID;
import org.apache.polaris.core.persistence.metrics.CommitMetricsRecord;
import org.apache.polaris.core.persistence.metrics.MetricsRecordIdentity;
import org.apache.polaris.core.persistence.metrics.ScanMetricsRecord;
import org.apache.polaris.core.persistence.pagination.EntityIdToken;
import org.apache.polaris.core.persistence.pagination.ImmutablePageToken;
import org.apache.polaris.core.persistence.pagination.Page;
import org.apache.polaris.core.persistence.pagination.PageToken;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

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
  private static final long OTHER_TABLE_ID = 201L;
  private static final List<MetricsQuerySpi.TableSelector> ALL_SNAPSHOTS =
      List.of(new MetricsQuerySpi.TableSelector(TABLE_ID, null));

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
    Page<MetricsRecordIdentity> scanPage =
        list(spi, Set.of(MetricsQuerySpi.MetricType.SCAN), ALL_SNAPSHOTS, PageToken.fromLimit(10));
    Page<MetricsRecordIdentity> commitPage =
        list(
            spi, Set.of(MetricsQuerySpi.MetricType.COMMIT), ALL_SNAPSHOTS, PageToken.fromLimit(10));

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

    Page<MetricsRecordIdentity> otherRealmPage =
        list(
            querySpi(realmB),
            Set.of(MetricsQuerySpi.MetricType.SCAN),
            ALL_SNAPSHOTS,
            PageToken.fromLimit(10));
    assertThat(otherRealmPage.items()).isEmpty();

    if (supportsPersistence()) {
      Page<MetricsRecordIdentity> ownRealmPage =
          list(
              querySpi(realmA),
              Set.of(MetricsQuerySpi.MetricType.SCAN),
              ALL_SNAPSHOTS,
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

    Page<MetricsRecordIdentity> firstPage =
        list(
            querySpi(realm),
            Set.of(MetricsQuerySpi.MetricType.SCAN),
            ALL_SNAPSHOTS,
            PageToken.fromLimit(2));
    assertThat(firstPage.items())
        .extracting(MetricsRecordIdentity::reportId)
        .containsExactly(newest.reportId(), middle.reportId());
    assertThat(firstPage.encodedResponseToken()).isNotNull();

    PageToken nextPageToken = PageToken.build(firstPage.encodedResponseToken(), 2, -1, () -> true);
    Page<MetricsRecordIdentity> secondPage =
        list(
            querySpi(realm), Set.of(MetricsQuerySpi.MetricType.SCAN), ALL_SNAPSHOTS, nextPageToken);
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

    Page<MetricsRecordIdentity> firstPage =
        list(
            querySpi(realm),
            Set.of(MetricsQuerySpi.MetricType.SCAN),
            ALL_SNAPSHOTS,
            PageToken.fromLimit(1));
    assertThat(firstPage.items())
        .extracting(MetricsRecordIdentity::reportId)
        .containsExactly(second.reportId());

    PageToken nextPageToken = PageToken.build(firstPage.encodedResponseToken(), 1, -1, () -> true);
    Page<MetricsRecordIdentity> secondPage =
        list(
            querySpi(realm), Set.of(MetricsQuerySpi.MetricType.SCAN), ALL_SNAPSHOTS, nextPageToken);
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
                list(
                    querySpi(realm()),
                    Set.of(MetricsQuerySpi.MetricType.SCAN),
                    ALL_SNAPSHOTS,
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

    Page<MetricsRecordIdentity> firstPage =
        list(
            querySpi(realmA),
            Set.of(MetricsQuerySpi.MetricType.SCAN),
            ALL_SNAPSHOTS,
            PageToken.fromLimit(1));
    assertThat(firstPage.encodedResponseToken()).isNotNull();

    // A cursor minted while listing realm A must not be honored when replayed against realm B,
    // even though catalog/table ids are identical: otherwise realm B's query would apply realm
    // A's keyset predicate to realm B's result set.
    PageToken replayedToken = PageToken.build(firstPage.encodedResponseToken(), 1, -1, () -> true);
    assertThatThrownBy(
            () ->
                list(
                    querySpi(realmB),
                    Set.of(MetricsQuerySpi.MetricType.SCAN),
                    ALL_SNAPSHOTS,
                    replayedToken))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @ParameterizedTest
  @EnumSource(MetricsQuerySpi.MetricType.class)
  void resultsContainOnlyRequestedMetricType(MetricsQuerySpi.MetricType metricType) {
    assumeTrue(supportsPersistence(), "backend does not persist data");
    String realm = realm();
    ScanMetricsRecord scan =
        scanRecord(UUID.randomUUID().toString(), CATALOG_ID, TABLE_ID, Instant.now());
    CommitMetricsRecord commit =
        commitRecord(UUID.randomUUID().toString(), CATALOG_ID, TABLE_ID, Instant.now(), 1L);
    writeScan(realm, scan);
    writeCommit(realm, commit);

    Page<MetricsRecordIdentity> page =
        list(querySpi(realm), Set.of(metricType), ALL_SNAPSHOTS, PageToken.fromLimit(10));

    assertThat(page.items())
        .extracting(MetricsRecordIdentity::reportId)
        .containsExactly(
            metricType == MetricsQuerySpi.MetricType.SCAN ? scan.reportId() : commit.reportId());
    assertThat(page.items())
        .allMatch(
            metricType == MetricsQuerySpi.MetricType.SCAN
                ? ScanMetricsRecord.class::isInstance
                : CommitMetricsRecord.class::isInstance);
  }

  @Test
  void multipleMetricTypesAreMergedAcrossPages() {
    assumeTrue(supportsPersistence(), "backend does not persist data");
    String realm = realm();
    Instant base = Instant.now();
    ScanMetricsRecord newest = scanRecord(UUID.randomUUID().toString(), CATALOG_ID, TABLE_ID, base);
    CommitMetricsRecord middle =
        commitRecord(UUID.randomUUID().toString(), CATALOG_ID, TABLE_ID, base.minusSeconds(1), 1L);
    ScanMetricsRecord oldest =
        scanRecord(UUID.randomUUID().toString(), CATALOG_ID, TABLE_ID, base.minusSeconds(2));
    writeScan(realm, oldest);
    writeCommit(realm, middle);
    writeScan(realm, newest);
    Set<MetricsQuerySpi.MetricType> bothTypes =
        Set.of(MetricsQuerySpi.MetricType.SCAN, MetricsQuerySpi.MetricType.COMMIT);

    Page<MetricsRecordIdentity> firstPage =
        list(querySpi(realm), bothTypes, ALL_SNAPSHOTS, PageToken.fromLimit(2));
    assertThat(firstPage.items())
        .extracting(MetricsRecordIdentity::reportId)
        .containsExactly(newest.reportId(), middle.reportId());
    assertThat(firstPage.encodedResponseToken()).isNotNull();

    PageToken nextPageToken = PageToken.build(firstPage.encodedResponseToken(), 2, -1, () -> true);
    Page<MetricsRecordIdentity> secondPage =
        list(querySpi(realm), bothTypes, ALL_SNAPSHOTS, nextPageToken);
    assertThat(secondPage.items())
        .extracting(MetricsRecordIdentity::reportId)
        .containsExactly(oldest.reportId());
    assertThat(secondPage.encodedResponseToken()).isNull();
  }

  @Test
  void tableSnapshotSelectorsMatchPairsWithoutDuplicates() {
    assumeTrue(supportsPersistence(), "backend does not persist data");
    String realm = realm();
    Instant base = Instant.now();
    CommitMetricsRecord t1s1 =
        commitRecord(UUID.randomUUID().toString(), CATALOG_ID, TABLE_ID, base, 1L);
    CommitMetricsRecord t1s2 =
        commitRecord(UUID.randomUUID().toString(), CATALOG_ID, TABLE_ID, base.minusSeconds(1), 2L);
    CommitMetricsRecord t2s1 =
        commitRecord(
            UUID.randomUUID().toString(), CATALOG_ID, OTHER_TABLE_ID, base.minusSeconds(2), 1L);
    CommitMetricsRecord t2s2 =
        commitRecord(
            UUID.randomUUID().toString(), CATALOG_ID, OTHER_TABLE_ID, base.minusSeconds(3), 2L);
    List.of(t1s1, t1s2, t2s1, t2s2).forEach(r -> writeCommit(realm, r));
    Set<MetricsQuerySpi.MetricType> commits = Set.of(MetricsQuerySpi.MetricType.COMMIT);

    // (t1, s1) OR (t2, s2) must not match the cross-product pairs (t1, s2) and (t2, s1).
    Page<MetricsRecordIdentity> pairs =
        list(
            querySpi(realm),
            commits,
            List.of(
                new MetricsQuerySpi.TableSelector(TABLE_ID, 1L),
                new MetricsQuerySpi.TableSelector(OTHER_TABLE_ID, 2L)),
            PageToken.fromLimit(10));
    assertThat(pairs.items())
        .extracting(MetricsRecordIdentity::reportId)
        .containsExactly(t1s1.reportId(), t2s2.reportId());

    // Overlapping selectors return each matching report once.
    Page<MetricsRecordIdentity> overlapping =
        list(
            querySpi(realm),
            commits,
            List.of(
                new MetricsQuerySpi.TableSelector(TABLE_ID, null),
                new MetricsQuerySpi.TableSelector(TABLE_ID, 1L)),
            PageToken.fromLimit(10));
    assertThat(overlapping.items())
        .extracting(MetricsRecordIdentity::reportId)
        .containsExactly(t1s1.reportId(), t1s2.reportId());
  }

  private static Page<MetricsRecordIdentity> list(
      MetricsQuerySpi spi,
      Set<MetricsQuerySpi.MetricType> metricTypes,
      List<MetricsQuerySpi.TableSelector> tables,
      PageToken pageToken) {
    return spi.listReports(metricTypes, CATALOG_ID, tables, null, null, pageToken);
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
    return commitRecord(reportId, catalogId, tableId, timestamp, 1L);
  }

  protected static CommitMetricsRecord commitRecord(
      String reportId, long catalogId, long tableId, Instant timestamp, long snapshotId) {
    return CommitMetricsRecord.builder()
        .reportId(reportId)
        .catalogId(catalogId)
        .tableId(tableId)
        .timestamp(timestamp)
        .snapshotId(snapshotId)
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
