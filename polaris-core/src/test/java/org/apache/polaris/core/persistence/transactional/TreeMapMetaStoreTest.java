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
package org.apache.polaris.core.persistence.transactional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.tuple;

import java.util.List;
import org.apache.polaris.core.PolarisDefaultDiagServiceImpl;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.entity.PolarisGrantRecord;
import org.apache.polaris.core.tag.CandidateBudget;
import org.apache.polaris.core.tag.TagAssignmentRecord;
import org.apache.polaris.core.tag.exceptions.CandidateBudgetExceededException;
import org.junit.jupiter.api.Test;

class TreeMapMetaStoreTest {

  private final PolarisDiagnostics diagnostics = new PolarisDefaultDiagServiceImpl();

  /**
   * A bounded range read must bound the copying, not the result. The unbounded read this replaced
   * also returns a short list when its caller stops early, so the size of what comes back proves
   * nothing; what the slice copied out of its map does. Fifty records under one prefix, a limit of
   * three, and three copies is the receipt.
   */
  @Test
  void readRangeWithALimitCopiesOnlyThatMany() {
    TreeMapMetaStore store = new TreeMapMetaStore(diagnostics);
    store.runActionInTransaction(
        diagnostics,
        () -> {
          for (int i = 0; i < 50; i++) {
            store.getSliceGrantRecords().write(new PolarisGrantRecord(1L, 2L, 3L, 100L + i, 5));
          }
        });
    long copiedAfterWrites = store.getSliceGrantRecords().copiedValueCount();

    List<PolarisGrantRecord> bounded =
        store.runInReadTransaction(
            diagnostics, () -> store.getSliceGrantRecords().readRange("", 3));

    assertThat(bounded).hasSize(3);
    assertThat(store.getSliceGrantRecords().copiedValueCount() - copiedAfterWrites).isEqualTo(3);

    // The bound is a bound, not a filter: the range still holds every record, and an unbounded read
    // of it copies all fifty, which is what the bounded read is measured against.
    long beforeUnbounded = store.getSliceGrantRecords().copiedValueCount();
    List<PolarisGrantRecord> whole =
        store.runInReadTransaction(diagnostics, () -> store.getSliceGrantRecords().readRange(""));
    assertThat(whole).hasSize(50);
    assertThat(store.getSliceGrantRecords().copiedValueCount() - beforeUnbounded).isEqualTo(50);

    // A limit past the end of the range is not an error and copies only what is there.
    long beforeGenerous = store.getSliceGrantRecords().copiedValueCount();
    List<PolarisGrantRecord> generous =
        store.runInReadTransaction(
            diagnostics, () -> store.getSliceGrantRecords().readRange("", Integer.MAX_VALUE));
    assertThat(generous).hasSize(50);
    assertThat(store.getSliceGrantRecords().copiedValueCount() - beforeGenerous).isEqualTo(50);
  }

  /** The bounded read keeps key order, so the same question stops at the same records. */
  @Test
  void readRangeWithALimitTakesTheFirstRecordsInKeyOrder() {
    TreeMapMetaStore store = new TreeMapMetaStore(diagnostics);
    store.runActionInTransaction(
        diagnostics,
        () -> {
          for (int i = 0; i < 10; i++) {
            store.getSliceGrantRecords().write(new PolarisGrantRecord(1L, 2L, 3L, 100L + i, 5));
          }
        });

    List<PolarisGrantRecord> firstThree =
        store.runInReadTransaction(
            diagnostics, () -> store.getSliceGrantRecords().readRange("", 3));
    List<PolarisGrantRecord> all =
        store.runInReadTransaction(diagnostics, () -> store.getSliceGrantRecords().readRange(""));

    assertThat(firstThree)
        .usingRecursiveFieldByFieldElementComparator()
        .isEqualTo(all.subList(0, 3));
  }

  @Test
  void readRangeReturnsCopiesForEmptyPrefix() {
    TreeMapMetaStore store = new TreeMapMetaStore(diagnostics);
    PolarisGrantRecord grantRecord = new PolarisGrantRecord(1L, 2L, 3L, 4L, 5);

    store.runActionInTransaction(
        diagnostics, () -> store.getSliceGrantRecords().write(grantRecord));

    List<PolarisGrantRecord> range =
        store.runInReadTransaction(diagnostics, () -> store.getSliceGrantRecords().readRange(""));
    range.get(0).setPrivilegeCode(99);

    PolarisGrantRecord stored =
        store.runInReadTransaction(
            diagnostics, () -> store.getSliceGrantRecords().readRange("").get(0));

    assertThat(stored.getPrivilegeCode()).isEqualTo(5);
  }

  @Test
  void readRangeReturnsCopiesForNonEmptyPrefix() {
    TreeMapMetaStore store = new TreeMapMetaStore(diagnostics);
    PolarisGrantRecord grantRecord = new PolarisGrantRecord(1L, 2L, 3L, 4L, 5);
    String prefix = store.buildPrefixKeyComposite(1L, 2L);

    store.runActionInTransaction(
        diagnostics, () -> store.getSliceGrantRecords().write(grantRecord));

    List<PolarisGrantRecord> range =
        store.runInReadTransaction(
            diagnostics, () -> store.getSliceGrantRecords().readRange(prefix));
    range.get(0).setPrivilegeCode(99);

    PolarisGrantRecord stored =
        store.runInReadTransaction(
            diagnostics, () -> store.getSliceGrantRecords().readRange(prefix).get(0));

    assertThat(stored.getPrivilegeCode()).isEqualTo(5);
  }

  /**
   * The by-tag index of tag assignments is read in {@code (targetId, fieldId)} order, and that
   * order is the map's own: no read sorts. Printed plainly, {@code 10} sorts before {@code 9}, so
   * this is the test that fails if the key ever stops being fixed-width -- the two rows come back
   * swapped and nothing downstream notices, because a page of the wrong rows is still a page.
   */
  @Test
  void tagAssignmentsByTagAreOrderedNumericallyNotLexicographically() {
    TreeMapMetaStore store = new TreeMapMetaStore(diagnostics);
    store.runActionInTransaction(
        diagnostics,
        () -> {
          // written in the order that is wrong both lexicographically and numerically, so neither
          // insertion order nor string order can pass this by accident
          store.getSliceTagAssignmentRecordsByTag().write(assignment(10L, 0, "v1"));
          store.getSliceTagAssignmentRecordsByTag().write(assignment(9L, 7, "v1"));
          store.getSliceTagAssignmentRecordsByTag().write(assignment(9L, 0, "v1"));
          // a synthetic row a caller writes to sort ahead of every generated entity id: the order
          // has to hold across the sign, not only among positives
          store.getSliceTagAssignmentRecordsByTag().write(assignment(-1L, 0, "v1"));
        });

    List<TagAssignmentRecord> inKeyOrder =
        store.runInReadTransaction(
            diagnostics,
            () ->
                store
                    .getSliceTagAssignmentRecordsByTag()
                    .readRange(TreeMapMetaStore.buildTagAssignmentByTagPrefix(TAG_CATALOG, TAG)));

    assertThat(inKeyOrder)
        .extracting(TagAssignmentRecord::getTargetId, TagAssignmentRecord::getFieldId)
        .containsExactly(tuple(-1L, 0), tuple(9L, 0), tuple(9L, 7), tuple(10L, 0));

    // A page resumes strictly after the row it names, by that same order.
    List<TagAssignmentRecord> afterFirst =
        store.runInReadTransaction(
            diagnostics,
            () ->
                store
                    .getSliceTagAssignmentRecordsByTag()
                    .readRange(
                        TreeMapMetaStore.buildTagAssignmentByTagPrefix(TAG_CATALOG, TAG),
                        TreeMapMetaStore.buildTagAssignmentByTagResumeKey(TAG_CATALOG, TAG, 9L, 0),
                        record -> true,
                        10));
    assertThat(afterFirst)
        .extracting(TagAssignmentRecord::getTargetId, TagAssignmentRecord::getFieldId)
        .containsExactly(tuple(9L, 7), tuple(10L, 0));

    // and resuming after the synthetic row reaches every real one
    assertThat(
            store.runInReadTransaction(
                diagnostics,
                () ->
                    store
                        .getSliceTagAssignmentRecordsByTag()
                        .readRange(
                            TreeMapMetaStore.buildTagAssignmentByTagPrefix(TAG_CATALOG, TAG),
                            TreeMapMetaStore.buildTagAssignmentByTagResumeKey(
                                TAG_CATALOG, TAG, -1L, 0),
                            record -> true,
                            10)))
        .extracting(TagAssignmentRecord::getTargetId, TagAssignmentRecord::getFieldId)
        .containsExactly(tuple(9L, 0), tuple(9L, 7), tuple(10L, 0));
  }

  /**
   * A page of a large definition copies a page, and the counters say which of the two things a
   * bound bounds. The size of the returned list proves neither: an unbounded read that its caller
   * truncates returns exactly as many rows.
   */
  @Test
  void aBoundedFilteredRangeReadCopiesOnlyThePageAndReportsWhatItVisited() {
    TreeMapMetaStore store = new TreeMapMetaStore(diagnostics);
    store.runActionInTransaction(
        diagnostics,
        () -> {
          for (int i = 0; i < 50; i++) {
            // only the last row carries "rare", so a filter for it has to walk the whole range
            store
                .getSliceTagAssignmentRecordsByTag()
                .write(assignment(100L + i, 0, i == 49 ? "rare" : "common"));
          }
        });
    String prefix = TreeMapMetaStore.buildTagAssignmentByTagPrefix(TAG_CATALOG, TAG);

    long copiedBefore = store.getSliceTagAssignmentRecordsByTag().copiedValueCount();
    long visitedBefore = store.getSliceTagAssignmentRecordsByTag().visitedValueCount();
    List<TagAssignmentRecord> unfiltered =
        store.runInReadTransaction(
            diagnostics,
            () ->
                store
                    .getSliceTagAssignmentRecordsByTag()
                    .readRange(prefix, null, record -> true, 2));

    // one page of one, plus the row that says a next page exists
    assertThat(unfiltered).hasSize(2);
    assertThat(store.getSliceTagAssignmentRecordsByTag().copiedValueCount() - copiedBefore)
        .isEqualTo(2);
    assertThat(store.getSliceTagAssignmentRecordsByTag().visitedValueCount() - visitedBefore)
        .isEqualTo(2);

    copiedBefore = store.getSliceTagAssignmentRecordsByTag().copiedValueCount();
    visitedBefore = store.getSliceTagAssignmentRecordsByTag().visitedValueCount();
    List<TagAssignmentRecord> filtered =
        store.runInReadTransaction(
            diagnostics,
            () ->
                store
                    .getSliceTagAssignmentRecordsByTag()
                    .readRange(prefix, null, record -> "rare".equals(record.getValue()), 2));

    // The filter is selective, so the read copies what it kept and compares what it had to. That is
    // the residual this bound does not remove, and it is the same walk a relational backend does
    // for
    // the same query under an ordered limit.
    assertThat(filtered).hasSize(1);
    assertThat(filtered.get(0).getTargetId()).isEqualTo(149L);
    assertThat(store.getSliceTagAssignmentRecordsByTag().copiedValueCount() - copiedBefore)
        .isEqualTo(1);
    assertThat(store.getSliceTagAssignmentRecordsByTag().visitedValueCount() - visitedBefore)
        .isEqualTo(50);

    // An unbounded read of the same range is what the bound is measured against.
    copiedBefore = store.getSliceTagAssignmentRecordsByTag().copiedValueCount();
    List<TagAssignmentRecord> whole =
        store.runInReadTransaction(
            diagnostics, () -> store.getSliceTagAssignmentRecordsByTag().readRange(prefix));
    assertThat(whole).hasSize(50);
    assertThat(store.getSliceTagAssignmentRecordsByTag().copiedValueCount() - copiedBefore)
        .isEqualTo(50);
  }

  /**
   * A value filter that rejects rows still examines them, and the examining is what the budget
   * bounds. Fifty rows of which only the last matches, a budget of twenty: the filter examines
   * twenty, is handed the twenty-first, and refuses it, rather than answering an empty list that
   * would read as the end of the range. The counters say what happened: the slice reached
   * twenty-one rows, the budget let the filter examine twenty, and none was copied.
   */
  @Test
  void aSparseFilterStopsAtTheVisitBudgetWithTheDistinguishedOutcome() {
    TreeMapMetaStore store = new TreeMapMetaStore(diagnostics);
    store.runActionInTransaction(
        diagnostics,
        () -> {
          for (int i = 0; i < 50; i++) {
            store
                .getSliceTagAssignmentRecordsByTag()
                .write(assignment(100L + i, 0, i == 49 ? "rare" : "common"));
          }
        });
    String prefix = TreeMapMetaStore.buildTagAssignmentByTagPrefix(TAG_CATALOG, TAG);
    CandidateBudget budget = CandidateBudget.of(20);
    long copiedBefore = store.getSliceTagAssignmentRecordsByTag().copiedValueCount();
    long visitedBefore = store.getSliceTagAssignmentRecordsByTag().visitedValueCount();

    assertThatThrownBy(
            () ->
                store.runInReadTransaction(
                    diagnostics,
                    () ->
                        store
                            .getSliceTagAssignmentRecordsByTag()
                            .readRange(prefix, null, budgeted(budget, "rare"), 2)))
        .isInstanceOf(CandidateBudgetExceededException.class);

    assertThat(store.getSliceTagAssignmentRecordsByTag().visitedValueCount() - visitedBefore)
        .isEqualTo(21);
    assertThat(store.getSliceTagAssignmentRecordsByTag().copiedValueCount() - copiedBefore)
        .isEqualTo(0);
    assertThat(budget.remaining()).isEqualTo(0);
  }

  /**
   * A range the budget covers is answered normally, matches or not: ten rows, none matching, a
   * budget of twenty, and the read returns its empty list having looked at ten. This is the outcome
   * the refusal above must never be mistaken for, so the two are pinned side by side.
   */
  @Test
  void aRangeScannedWithinItsBudgetAnswersNormally() {
    TreeMapMetaStore store = new TreeMapMetaStore(diagnostics);
    store.runActionInTransaction(
        diagnostics,
        () -> {
          for (int i = 0; i < 10; i++) {
            store.getSliceTagAssignmentRecordsByTag().write(assignment(100L + i, 0, "common"));
          }
        });
    String prefix = TreeMapMetaStore.buildTagAssignmentByTagPrefix(TAG_CATALOG, TAG);
    CandidateBudget budget = CandidateBudget.of(20);
    long visitedBefore = store.getSliceTagAssignmentRecordsByTag().visitedValueCount();

    List<TagAssignmentRecord> none =
        store.runInReadTransaction(
            diagnostics,
            () ->
                store
                    .getSliceTagAssignmentRecordsByTag()
                    .readRange(prefix, null, budgeted(budget, "rare"), 2));

    assertThat(none).isEmpty();
    assertThat(store.getSliceTagAssignmentRecordsByTag().visitedValueCount() - visitedBefore)
        .isEqualTo(10);
    assertThat(budget.remaining()).isEqualTo(10);
  }

  /**
   * The row read past the page, the one that says a next page exists, is examined like any other
   * and costs one unit. Three matching rows, a page of one plus its lookahead, a budget of exactly
   * two: both are read and the budget is spent to the last unit, with no refusal. With a budget of
   * one the lookahead cannot be afforded, and the read refuses rather than answering the single row
   * as though it were the last.
   */
  @Test
  void theLookaheadRowIsChargedLikeAnyOther() {
    TreeMapMetaStore store = new TreeMapMetaStore(diagnostics);
    store.runActionInTransaction(
        diagnostics,
        () -> {
          for (int i = 0; i < 3; i++) {
            store.getSliceTagAssignmentRecordsByTag().write(assignment(100L + i, 0, "v1"));
          }
        });
    String prefix = TreeMapMetaStore.buildTagAssignmentByTagPrefix(TAG_CATALOG, TAG);

    CandidateBudget exactlyThePage = CandidateBudget.of(2);
    List<TagAssignmentRecord> pageAndLookahead =
        store.runInReadTransaction(
            diagnostics,
            () ->
                store
                    .getSliceTagAssignmentRecordsByTag()
                    .readRange(prefix, null, budgeted(exactlyThePage, "v1"), 2));
    assertThat(pageAndLookahead).hasSize(2);
    assertThat(exactlyThePage.remaining()).isEqualTo(0);

    CandidateBudget oneShort = CandidateBudget.of(1);
    assertThatThrownBy(
            () ->
                store.runInReadTransaction(
                    diagnostics,
                    () ->
                        store
                            .getSliceTagAssignmentRecordsByTag()
                            .readRange(prefix, null, budgeted(oneShort, "v1"), 2)))
        .isInstanceOf(CandidateBudgetExceededException.class);
    assertThat(oneShort.remaining()).isEqualTo(0);
  }

  /** The value filter the persistence layer builds: charge the budget, then look at the row. */
  private static java.util.function.Predicate<TagAssignmentRecord> budgeted(
      CandidateBudget budget, String value) {
    return record -> {
      budget.examine();
      return value.equals(record.getValue());
    };
  }

  private static final long TAG_CATALOG = 1L;
  private static final long TAG = 2L;

  private static TagAssignmentRecord assignment(long targetId, int fieldId, String value) {
    return new TagAssignmentRecord(TAG_CATALOG, targetId, fieldId, TAG_CATALOG, TAG, value);
  }
}
