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
package org.apache.polaris.persistence.nosql.metastore.maintenance;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import java.time.Duration;
import org.junit.jupiter.api.Test;

class TestCommitRetention {

  @Test
  void stopsAfterConfiguredNumberOfCommits() {
    var continueAfterCommit =
        CatalogRetainedIdentifier.<Object>historyContinuePredicate(
            1, Duration.ZERO, false, ignored -> Duration.ZERO);
    // The current commit has already been retained, so no second commit is needed.
    assertThat(continueAfterCommit.test(new Object())).isFalse();

    continueAfterCommit =
        CatalogRetainedIdentifier.historyContinuePredicate(
            3, Duration.ZERO, false, ignored -> Duration.ZERO);
    assertThat(continueAfterCommit.test(new Object())).isTrue();
    assertThat(continueAfterCommit.test(new Object())).isTrue();
    // The third commit has already been retained, so traversal stops here.
    assertThat(continueAfterCommit.test(new Object())).isFalse();
    // Further calls remain false without underflowing the counter.
    assertThat(continueAfterCommit.test(new Object())).isFalse();
  }

  @Test
  void rejectsInvalidCommitCount() {
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                CatalogRetainedIdentifier.historyContinuePredicate(
                    0, Duration.ZERO, false, ignored -> Duration.ZERO))
        .withMessage("commitsToRetain must be at least 1");
  }

  @Test
  void continuesWhileNextCommitIsWithinRetentionDuration() {
    var continueAfterCommit =
        CatalogRetainedIdentifier.<Duration>historyContinuePredicate(
            1, Duration.ofDays(30), false, age -> age);

    assertThat(continueAfterCommit.test(Duration.ofDays(29))).isTrue();
    assertThat(continueAfterCommit.test(Duration.ofDays(30))).isFalse();
  }

  @Test
  void combinesCountAndRetentionDuration() {
    var continueAfterCommit =
        CatalogRetainedIdentifier.<Duration>historyContinuePredicate(
            3, Duration.ofDays(30), false, age -> age);

    assertThat(continueAfterCommit.test(Duration.ofDays(40))).isTrue();
    assertThat(continueAfterCommit.test(Duration.ofDays(40))).isTrue();
    assertThat(continueAfterCommit.test(Duration.ofDays(29))).isTrue();
    assertThat(continueAfterCommit.test(Duration.ofDays(30))).isFalse();
  }

  @Test
  void usesLongerOfHistoryAndPaginationRetention() {
    var continueAfterCommit =
        CatalogRetainedIdentifier.<Duration>paginatedHistoryContinuePredicate(
            1, Duration.ofDays(3), false, Duration.ofDays(30), age -> age);

    assertThat(continueAfterCommit.test(Duration.ofDays(29))).isTrue();
    assertThat(continueAfterCommit.test(Duration.ofDays(30))).isFalse();
  }

  @Test
  void doesNotApplyPaginationRetentionToNonPaginatedHistory() {
    var nonPaginatedHistory =
        CatalogRetainedIdentifier.<Duration>historyContinuePredicate(
            1, Duration.ofDays(7), false, age -> age);
    var paginatedHistory =
        CatalogRetainedIdentifier.<Duration>paginatedHistoryContinuePredicate(
            1, Duration.ofDays(7), false, Duration.ofDays(30), age -> age);

    assertThat(nonPaginatedHistory.test(Duration.ofDays(8))).isFalse();
    assertThat(paginatedHistory.test(Duration.ofDays(8))).isTrue();
  }

  @Test
  void retainsAllCommits() {
    var continueAfterCommit =
        CatalogRetainedIdentifier.<Object>historyContinuePredicate(
            1, Duration.ZERO, true, ignored -> Duration.ZERO);

    assertThat(continueAfterCommit.test(new Object())).isTrue();
    assertThat(continueAfterCommit.test(new Object())).isTrue();
  }

  @Test
  void rejectsNegativeRetentionDuration() {
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                CatalogRetainedIdentifier.historyContinuePredicate(
                    1, Duration.ofSeconds(-1), false, ignored -> Duration.ZERO))
        .withMessage("minimumRetention must not be negative");
  }

  @Test
  void rejectsNegativePaginationTokenRetention() {
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                CatalogRetainedIdentifier.paginatedHistoryContinuePredicate(
                    1, Duration.ZERO, false, Duration.ofSeconds(-1), ignored -> Duration.ZERO))
        .withMessage("paginationTokenRetention must not be negative");
  }
}
