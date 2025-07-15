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
package org.apache.polaris.persistence.nosql.impl.commits;

import static org.apache.polaris.persistence.nosql.api.obj.ObjRef.objRef;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.polaris.persistence.nosql.api.Persistence;
import org.apache.polaris.persistence.nosql.api.commit.CommitException;
import org.apache.polaris.persistence.nosql.api.exceptions.ReferenceNotFoundException;
import org.apache.polaris.persistence.nosql.api.obj.AnotherTestObj;
import org.apache.polaris.persistence.nosql.api.obj.CommitTestObj;
import org.apache.polaris.persistence.nosql.api.obj.Obj;
import org.apache.polaris.persistence.nosql.api.obj.ObjRef;
import org.apache.polaris.persistence.nosql.api.obj.VersionedTestObj;
import org.apache.polaris.persistence.nosql.testextension.PersistenceTestExtension;
import org.apache.polaris.persistence.nosql.testextension.PolarisPersistence;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith({PersistenceTestExtension.class, SoftAssertionsExtension.class})
public abstract class BaseTestCommitterImpl {
  @InjectSoftAssertions protected SoftAssertions soft;

  @PolarisPersistence(fastRetries = true)
  protected Persistence persistence;

  @Test
  public void committerStateImpl() {
    var state = new CommitterImpl.CommitterStateImpl<CommitTestObj, String>(persistence);
    var o1 =
        CommitTestObj.builder()
            .id(persistence.generateId())
            .text("simple 1")
            .seq(1L)
            .tail(new long[0])
            .build();
    var o2 = AnotherTestObj.builder().id(persistence.generateId()).text("another 2").build();
    var o1b =
        VersionedTestObj.builder()
            .from(o1)
            .id(persistence.generateId())
            .someValue("another 1 b")
            .build();

    soft.assertThat(state.getWrittenByKey("one")).isNull();
    soft.assertThatCode(() -> state.writeIntent("one", o1)).doesNotThrowAnyException();
    soft.assertThat(state.idsUsed).containsExactly(objRef(o1));
    soft.assertThatIllegalStateException()
        .isThrownBy(() -> state.writeIntent("one", o2))
        .withMessage("The object-key 'one' has already been used");
    soft.assertThat(state.deleteIds).isEmpty();
    soft.assertThat(state.idsUsed).containsExactly(objRef(o1));
    soft.assertThat(state.forAttempt).containsExactly(Map.entry(objRef(o1), o1));

    soft.assertThatIllegalStateException()
        .isThrownBy(() -> state.writeIntent("two", o1))
        .withMessageStartingWith("Object ID '")
        .withMessageContaining("' to be persisted has already been used. ");
    soft.assertThat(state.getWrittenByKey("two")).isNull();
    soft.assertThat(state.deleteIds).isEmpty();
    soft.assertThat(state.idsUsed).containsExactly(objRef(o1));
    soft.assertThat(state.forAttempt).containsExactly(Map.entry(objRef(o1), o1));

    soft.assertThatIllegalStateException()
        .isThrownBy(() -> state.writeIfNew("two", o1))
        .withMessageStartingWith("Object ID '")
        .withMessageContaining("' to be persisted has already been used. ");
    soft.assertThat(state.getWrittenByKey("two")).isNull();
    soft.assertThat(state.deleteIds).isEmpty();
    soft.assertThat(state.idsUsed).containsExactly(objRef(o1));
    soft.assertThat(state.forAttempt).containsExactly(Map.entry(objRef(o1), o1));

    soft.assertThatIllegalStateException()
        .isThrownBy(() -> state.writeOrReplace("two", o1))
        .withMessageStartingWith("Object ID '")
        .withMessageContaining("' to be persisted has already been used. ");
    soft.assertThat(state.getWrittenByKey("two")).isNull();
    soft.assertThat(state.deleteIds).isEmpty();
    soft.assertThat(state.idsUsed).containsExactly(objRef(o1));
    soft.assertThat(state.forAttempt).containsExactly(Map.entry(objRef(o1), o1));

    soft.assertThat(state.getWrittenByKey("one")).isSameAs(o1);
    soft.assertThat(state.writeIfNew("one", o1b)).isSameAs(o1);
    soft.assertThat(state.deleteIds).isEmpty();
    soft.assertThat(state.idsUsed).containsExactly(objRef(o1));
    soft.assertThat(state.forAttempt).containsExactly(Map.entry(objRef(o1), o1));

    soft.assertThat(state.getWrittenByKey("one")).isSameAs(o1);
    soft.assertThat(state.writeOrReplace("one", o2)).isSameAs(o2);
    soft.assertThat(state.deleteIds).containsExactly(objRef(o1));
    soft.assertThat(state.idsUsed).containsExactlyInAnyOrder(objRef(o1), objRef(o2));
    soft.assertThat(state.forAttempt).containsExactly(Map.entry(objRef(o2), o2));

    soft.assertThat(state.getWrittenByKey("one")).isSameAs(o2);
    soft.assertThat(state.writeOrReplace("one", o1b)).isSameAs(o1b);
    soft.assertThat(state.deleteIds).containsExactlyInAnyOrder(objRef(o1), objRef(o2));
    soft.assertThat(state.idsUsed).containsExactlyInAnyOrder(objRef(o1), objRef(o2), objRef(o1b));
    soft.assertThat(state.forAttempt).containsExactly(Map.entry(objRef(o1b), o1b));
  }

  @Test
  public void simpleCase(TestInfo testInfo) throws Exception {
    var initialObj =
        persistence.write(
            CommitTestObj.builder()
                .id(persistence.generateId())
                .text("initial")
                .seq(1)
                .tail(new long[0])
                .build(),
            CommitTestObj.class);
    var referenceName = testInfo.getTestMethod().orElseThrow().getName();

    // Prepare, create reference with initial object

    persistence.write(initialObj, CommitTestObj.class);
    persistence.createReference(referenceName, Optional.of(objRef(initialObj)));

    var committed =
        persistence
            .createCommitter(referenceName, CommitTestObj.class, String.class)
            .commit(
                (state, refObjSupplier) -> {
                  var refObj = refObjSupplier.get();
                  soft.assertThat(refObj).get().isEqualTo(initialObj).isNotSameAs(initialObj);

                  // Commit attempt works here
                  return state.commitResult(
                      "foo", CommitTestObj.builder().text("result text"), refObj);
                });

    soft.assertThat(committed).contains("foo");
    var newHead = persistence.fetchReferenceHead(referenceName, CommitTestObj.class);
    soft.assertThat(newHead)
        .get()
        .extracting(CommitTestObj::text, CommitTestObj::seq, CommitTestObj::tail)
        .containsExactly("result text", 2L, new long[] {initialObj.id()});

    var notCommitted =
        persistence
            .createCommitter(referenceName, CommitTestObj.class, String.class)
            .commit(
                (state, refObjSupplier) -> {
                  var refObj = refObjSupplier.get();
                  soft.assertThat(refObj).get().isNotEqualTo(initialObj);

                  // Commit attempt works here
                  return state.noCommit();
                });
    soft.assertThat(notCommitted).isEmpty();

    var checkHead = persistence.fetchReferenceHead(referenceName, CommitTestObj.class);
    soft.assertThat(checkHead).isEqualTo(newHead);

    var notCommittedWithResult =
        persistence
            .createCommitter(referenceName, CommitTestObj.class, String.class)
            .commit(
                (state, refObjSupplier) -> {
                  var refObj = refObjSupplier.get();
                  soft.assertThat(refObj).get().isNotEqualTo(initialObj);

                  // Commit attempt works here
                  return state.noCommit("not committed");
                });
    soft.assertThat(notCommittedWithResult).contains("not committed");

    var checkHead2 = persistence.fetchReferenceHead(referenceName, CommitTestObj.class);
    soft.assertThat(checkHead2).isEqualTo(newHead);
  }

  @Test
  @SuppressWarnings("ReturnValueIgnored")
  public void nonExistingReferenceThrows(TestInfo testInfo) {
    var referenceName = testInfo.getTestMethod().orElseThrow().getName();

    soft.assertThatThrownBy(
            () ->
                persistence
                    .createCommitter(referenceName, CommitTestObj.class, String.class)
                    .commit(
                        (state, refObjSupplier) -> {
                          refObjSupplier.get();
                          soft.fail("Must not be call");
                          return Optional.of(
                              CommitTestObj.builder()
                                  .id(persistence.generateId())
                                  .text("initial")
                                  .build());
                        }))
        .isInstanceOf(ReferenceNotFoundException.class);
  }

  @Test
  public void simpleImmediatelySuccessfulCommit(TestInfo testInfo) throws Exception {
    var initialObj =
        persistence.write(
            CommitTestObj.builder()
                .id(persistence.generateId())
                .text("initial")
                .seq(1)
                .tail(new long[0])
                .build(),
            CommitTestObj.class);
    var referenceName = testInfo.getTestMethod().orElseThrow().getName();

    var anotherObj1 =
        AnotherTestObj.builder().id(persistence.generateId()).text("another 1").build();
    var anotherObj2 =
        AnotherTestObj.builder().id(persistence.generateId()).text("another 2").build();

    persistence.write(initialObj, CommitTestObj.class);
    persistence.createReference(referenceName, Optional.of(objRef(initialObj)));

    soft.assertThat(
            persistence.fetchMany(
                Obj.class,
                objRef(anotherObj1.withNumParts(1)),
                objRef(anotherObj2.withNumParts(1))))
        .containsOnlyNulls();

    soft.assertThat(
            persistence
                .createCommitter(referenceName, CommitTestObj.class, String.class)
                .commit(
                    (state, refObjSupplier) -> {
                      var refObj = refObjSupplier.get();
                      soft.assertThat(refObj).get().isEqualTo(initialObj).isNotSameAs(initialObj);
                      soft.assertThat(state.writeIfNew("another 1", anotherObj1))
                          .isSameAs(anotherObj1);
                      soft.assertThat(state.writeIfNew("another 2", anotherObj2))
                          .isSameAs(anotherObj2);
                      return state.commitResult(
                          "foo", CommitTestObj.builder().text("result"), refObj);
                    }))
        .contains("foo");

    soft.assertThat(
            persistence.fetchMany(
                Obj.class,
                objRef(anotherObj1.withNumParts(1)),
                objRef(anotherObj2.withNumParts(1))))
        .containsExactly(anotherObj1.withNumParts(1), anotherObj2.withNumParts(1));
  }

  @Test
  public void writeIntentSuccessfulCommitAfterFourRetries(TestInfo testInfo) throws Exception {
    var initialObj =
        persistence.write(
            CommitTestObj.builder()
                .id(persistence.generateId())
                .text("initial")
                .seq(1)
                .tail(new long[0])
                .build(),
            CommitTestObj.class);
    var referenceName = testInfo.getTestMethod().orElseThrow().getName();

    var createdObjs = new ArrayList<ObjRef>();
    var expectedObjs = new ArrayList<ObjRef>();
    var iteration = new AtomicInteger();

    persistence.write(initialObj, CommitTestObj.class);
    persistence.createReference(referenceName, Optional.of(objRef(initialObj)));

    soft.assertThat(
            persistence
                .createCommitter(referenceName, CommitTestObj.class, String.class)
                .commit(
                    (state, refObjSupplier) -> {
                      var refObj = refObjSupplier.get();
                      soft.assertThat(refObj).get().isEqualTo(initialObj).isNotSameAs(initialObj);

                      var attempt = iteration.incrementAndGet();

                      if (attempt == 1) {

                        var anotherObj1 =
                            AnotherTestObj.builder()
                                .id(persistence.generateId())
                                .text("another 1, attempt " + attempt)
                                .build();
                        var anotherObj2 =
                            AnotherTestObj.builder()
                                .id(persistence.generateId())
                                .text("another 2, attempt " + attempt)
                                .build();

                        createdObjs.add(objRef(anotherObj1));
                        createdObjs.add(objRef(anotherObj2));

                        soft.assertThat(state.getWrittenByKey("another 1")).isNull();
                        soft.assertThat(state.getWrittenByKey("another 2")).isNull();

                        soft.assertThatCode(() -> state.writeIntent("another 1", anotherObj1))
                            .doesNotThrowAnyException();
                        soft.assertThatCode(() -> state.writeIntent("another 2", anotherObj2))
                            .doesNotThrowAnyException();
                        expectedObjs.add(objRef(anotherObj1));
                        expectedObjs.add(objRef(anotherObj2));
                      } else {
                        soft.assertThat(state.getWrittenByKey("another 1")).isNotNull();
                        soft.assertThat(state.getWrittenByKey("another 2")).isNotNull();
                      }

                      if (attempt < 4) {
                        // retry
                        return Optional.empty();
                      }

                      var resultObj = CommitTestObj.builder().text("result");

                      var r = state.commitResult("foo", resultObj, refObj);
                      expectedObjs.add(objRef(r.orElseThrow()));
                      return r;
                    }))
        .contains("foo");

    soft.assertThat(expectedObjs).hasSize(3).doesNotHaveDuplicates();

    // 4 attempts, 2 x 'AnotherTestObj'
    soft.assertThat(createdObjs).hasSize(2).doesNotHaveDuplicates();

    var unexpectedObjs = new HashSet<>(createdObjs);
    expectedObjs.forEach(unexpectedObjs::remove);
    soft.assertThat(unexpectedObjs).isEmpty();

    soft.assertThat(persistence.fetchMany(Obj.class, withPartNum1(expectedObjs)))
        .hasSize(3)
        .doesNotContainNull();
    soft.assertThat(persistence.fetch(objRef(initialObj.withNumParts(1)), CommitTestObj.class))
        .isEqualTo(initialObj.withNumParts(1));
  }

  @Test
  public void writeIfNewSuccessfulCommitAfterFourRetries(TestInfo testInfo) throws Exception {
    var initialObj =
        persistence.write(
            CommitTestObj.builder()
                .id(persistence.generateId())
                .text("initial")
                .seq(1)
                .tail(new long[0])
                .build(),
            CommitTestObj.class);
    var referenceName = testInfo.getTestMethod().orElseThrow().getName();

    var createdObjs = new ArrayList<ObjRef>();
    var expectedObjs = new ArrayList<ObjRef>();
    var iteration = new AtomicInteger();

    persistence.write(initialObj, CommitTestObj.class);
    persistence.createReference(referenceName, Optional.of(objRef(initialObj)));

    soft.assertThat(
            persistence
                .createCommitter(referenceName, CommitTestObj.class, String.class)
                .commit(
                    (state, refObjSupplier) -> {
                      var refObj = refObjSupplier.get();
                      soft.assertThat(refObj).get().isEqualTo(initialObj).isNotSameAs(initialObj);

                      var attempt = iteration.incrementAndGet();

                      var anotherObj1 =
                          AnotherTestObj.builder()
                              .id(persistence.generateId())
                              .text("another 1, attempt " + attempt)
                              .build();
                      var anotherObj2 =
                          AnotherTestObj.builder()
                              .id(persistence.generateId())
                              .text("another 2, attempt " + attempt)
                              .build();

                      createdObjs.add(objRef(anotherObj1));
                      createdObjs.add(objRef(anotherObj2));

                      if (attempt == 1) {
                        soft.assertThat(state.getWrittenByKey("another 1")).isNull();
                        soft.assertThat(state.getWrittenByKey("another 2")).isNull();

                        soft.assertThat(state.writeIfNew("another 1", anotherObj1))
                            .isSameAs(anotherObj1);
                        soft.assertThat(state.writeIfNew("another 2", anotherObj2))
                            .isSameAs(anotherObj2);
                        expectedObjs.add(objRef(anotherObj1));
                        expectedObjs.add(objRef(anotherObj2));
                      } else {
                        soft.assertThat(state.getWrittenByKey("another 1")).isNotNull();
                        soft.assertThat(state.getWrittenByKey("another 2")).isNotNull();

                        soft.assertThat(state.writeIfNew("another 1", anotherObj1))
                            .isNotEqualTo(anotherObj1);
                        soft.assertThat(state.writeIfNew("another 2", anotherObj2))
                            .isNotEqualTo(anotherObj2);
                      }

                      if (attempt < 4) {
                        // retry
                        return Optional.empty();
                      }

                      var resultObj = CommitTestObj.builder().text("result");

                      var r = state.commitResult("foo", resultObj, refObj);
                      expectedObjs.add(objRef(r.orElseThrow()));
                      return r;
                    }))
        .contains("foo");

    soft.assertThat(expectedObjs).hasSize(3).doesNotHaveDuplicates();

    // 4 attempts, 2 x 'AnotherTestObj'
    soft.assertThat(createdObjs).hasSize(4 * 2).doesNotHaveDuplicates();

    var unexpectedObjs = new HashSet<>(createdObjs);
    expectedObjs.forEach(unexpectedObjs::remove);
    soft.assertThat(unexpectedObjs).hasSize(6);

    soft.assertThat(persistence.fetchMany(Obj.class, withPartNum1(unexpectedObjs)))
        .hasSize(6)
        .containsOnlyNulls();
    soft.assertThat(persistence.fetchMany(Obj.class, withPartNum1(expectedObjs)))
        .hasSize(3)
        .doesNotContainNull();
    soft.assertThat(persistence.fetch(objRef(initialObj.withNumParts(1)), CommitTestObj.class))
        .isEqualTo(initialObj.withNumParts(1));
  }

  @Test
  public void writeOrReplaceSuccessfulCommitAfterFourRetries(TestInfo testInfo) throws Exception {
    var initialObj =
        persistence.write(
            CommitTestObj.builder()
                .id(persistence.generateId())
                .text("initial")
                .seq(1)
                .tail(new long[0])
                .build(),
            CommitTestObj.class);
    var referenceName = testInfo.getTestMethod().orElseThrow().getName();

    var createdObjs = new ArrayList<ObjRef>();
    var expectedObjs = new ArrayList<ObjRef>();
    var iteration = new AtomicInteger();

    persistence.write(initialObj, CommitTestObj.class);
    persistence.createReference(referenceName, Optional.of(objRef(initialObj)));

    soft.assertThat(
            persistence
                .createCommitter(referenceName, CommitTestObj.class, String.class)
                .commit(
                    (state, refObjSupplier) -> {
                      var refObj = refObjSupplier.get();
                      soft.assertThat(refObj).get().isEqualTo(initialObj).isNotSameAs(initialObj);

                      var attempt = iteration.incrementAndGet();

                      var anotherObj1 =
                          AnotherTestObj.builder()
                              .id(persistence.generateId())
                              .text("another 1, attempt " + attempt)
                              .build();
                      var anotherObj2 =
                          AnotherTestObj.builder()
                              .id(persistence.generateId())
                              .text("another 2, attempt " + attempt)
                              .build();

                      createdObjs.add(objRef(anotherObj1));
                      createdObjs.add(objRef(anotherObj2));

                      if (attempt == 1) {
                        soft.assertThat(state.getWrittenByKey("another 1")).isNull();
                        soft.assertThat(state.getWrittenByKey("another 2")).isNull();
                      } else {
                        soft.assertThat(state.getWrittenByKey("another 1")).isNotNull();
                        soft.assertThat(state.getWrittenByKey("another 2")).isNotNull();
                      }

                      state.writeOrReplace("another 1", anotherObj1);
                      state.writeOrReplace("another 2", anotherObj2);

                      soft.assertThat(state.getWrittenByKey("another 1")).isNotNull();
                      soft.assertThat(state.getWrittenByKey("another 2")).isNotNull();

                      if (attempt < 4) {
                        // retry
                        return Optional.empty();
                      }

                      var resultObj = CommitTestObj.builder().text("result");

                      expectedObjs.add(objRef(anotherObj1));
                      expectedObjs.add(objRef(anotherObj2));

                      var r = state.commitResult("foo", resultObj, refObj);

                      expectedObjs.add(objRef(r.orElseThrow()));

                      return r;
                    }))
        .contains("foo");

    soft.assertThat(expectedObjs).hasSize(3).doesNotHaveDuplicates();

    // 4 attempts, 2 x 'AnotherTestObj'
    soft.assertThat(createdObjs).hasSize(4 * 2).doesNotHaveDuplicates();

    var unexpectedObjs = new HashSet<>(createdObjs);
    expectedObjs.forEach(unexpectedObjs::remove);
    soft.assertThat(unexpectedObjs).hasSize(6);

    soft.assertThat(persistence.fetchMany(Obj.class, withPartNum1(unexpectedObjs)))
        .hasSize(6)
        .containsOnlyNulls();
    soft.assertThat(persistence.fetchMany(Obj.class, withPartNum1(expectedObjs)))
        .hasSize(3)
        .doesNotContainNull();
    soft.assertThat(persistence.fetch(objRef(initialObj.withNumParts(1)), CommitTestObj.class))
        .isEqualTo(initialObj);
  }

  @Test
  public void failingCommitMustDeleteAllObjs(TestInfo testInfo) {
    var initialObj =
        persistence.write(
            CommitTestObj.builder()
                .id(persistence.generateId())
                .text("initial")
                .seq(1)
                .tail(new long[0])
                .build(),
            CommitTestObj.class);
    var referenceName = testInfo.getTestMethod().orElseThrow().getName();

    var createdObjs = new ArrayList<ObjRef>();
    var iteration = new AtomicInteger();

    persistence.write(initialObj, CommitTestObj.class);
    persistence.createReference(referenceName, Optional.of(objRef(initialObj)));

    soft.assertThatThrownBy(
            () ->
                persistence
                    .createCommitter(referenceName, CommitTestObj.class, String.class)
                    .commit(
                        (state, refObjSupplier) -> {
                          soft.assertThat(refObjSupplier.get())
                              .get()
                              .isEqualTo(initialObj)
                              .isNotSameAs(initialObj);

                          var attempt = iteration.incrementAndGet();

                          var anotherObj1 =
                              AnotherTestObj.builder()
                                  .id(persistence.generateId())
                                  .text("another 1, attempt " + attempt)
                                  .build();
                          var anotherObj2 =
                              AnotherTestObj.builder()
                                  .id(persistence.generateId())
                                  .text("another 2, attempt " + attempt)
                                  .build();

                          createdObjs.add(objRef(anotherObj1));
                          createdObjs.add(objRef(anotherObj2));

                          state.writeOrReplace("another 1 / " + attempt, anotherObj1);
                          state.writeOrReplace("another 2 / " + attempt, anotherObj2);

                          if (attempt < 4) {
                            // retry
                            return Optional.empty();
                          }

                          throw new CommitException("failed commit") {};
                        }))
        .isInstanceOf(CommitException.class)
        .hasMessage("failed commit");

    // 4 attempts, 2 x 'AnotherTestObj'
    soft.assertThat(createdObjs).hasSize(4 * 2).doesNotHaveDuplicates();

    soft.assertThat(persistence.fetchMany(Obj.class, withPartNum1(createdObjs)))
        .hasSize(8)
        .containsOnlyNulls();
    soft.assertThat(persistence.fetch(objRef(initialObj.withNumParts(1)), CommitTestObj.class))
        .isEqualTo(initialObj.withNumParts(1));
  }

  @Test
  public void sameRefPointerMustNotWriteObjs(TestInfo testInfo) {
    var initialObj =
        persistence.write(
            CommitTestObj.builder()
                .id(persistence.generateId())
                .text("initial")
                .seq(1)
                .tail(new long[0])
                .build(),
            CommitTestObj.class);
    var referenceName = testInfo.getTestMethod().orElseThrow().getName();

    var createdObjs = new ArrayList<ObjRef>();
    var iteration = new AtomicInteger();

    persistence.write(initialObj, CommitTestObj.class);
    persistence.createReference(referenceName, Optional.of(objRef(initialObj)));

    soft.assertThatIllegalStateException()
        .isThrownBy(
            () ->
                persistence
                    .createCommitter(referenceName, CommitTestObj.class, String.class)
                    .commit(
                        (state, refObjSupplier) -> {
                          soft.assertThat(refObjSupplier.get())
                              .get()
                              .isEqualTo(initialObj)
                              .isNotSameAs(initialObj);

                          var attempt = iteration.incrementAndGet();

                          var anotherObj1 =
                              AnotherTestObj.builder()
                                  .id(persistence.generateId())
                                  .text("another 1, attempt " + attempt)
                                  .build();
                          var anotherObj2 =
                              AnotherTestObj.builder()
                                  .id(persistence.generateId())
                                  .text("another 2, attempt " + attempt)
                                  .build();

                          createdObjs.add(objRef(anotherObj1));
                          createdObjs.add(objRef(anotherObj2));

                          state.writeOrReplace("another 1 / " + attempt, anotherObj1);
                          state.writeOrReplace("another 2 / " + attempt, anotherObj2);

                          if (attempt < 4) {
                            // retry
                            return Optional.empty();
                          }

                          return Optional.of(initialObj);
                        }))
        .withMessage(
            "CommitRetryable.attempt() returned the current reference's pointer, in this case it must not attempt to persist any objects");

    // 4 attempts, 2 x 'AnotherTestObj'
    soft.assertThat(createdObjs).hasSize(4 * 2).doesNotHaveDuplicates();

    soft.assertThat(persistence.fetchMany(Obj.class, withPartNum1(createdObjs)))
        .hasSize(8)
        .containsOnlyNulls();
    soft.assertThat(persistence.fetch(objRef(initialObj.withNumParts(1)), CommitTestObj.class))
        .isEqualTo(initialObj.withNumParts(1));
  }

  @Test
  public void sameRefPointerMustNotModify(TestInfo testInfo) {
    var initialObj =
        persistence.write(
            CommitTestObj.builder()
                .id(persistence.generateId())
                .text("initial")
                .seq(1)
                .tail(new long[0])
                .build(),
            CommitTestObj.class);
    var referenceName = testInfo.getTestMethod().orElseThrow().getName();

    var iteration = new AtomicInteger();

    persistence.write(initialObj, CommitTestObj.class);
    persistence.createReference(referenceName, Optional.of(objRef(initialObj)));

    soft.assertThatIllegalStateException()
        .isThrownBy(
            () ->
                persistence
                    .createCommitter(referenceName, CommitTestObj.class, String.class)
                    .commit(
                        (state, refObjSupplier) -> {
                          soft.assertThat(refObjSupplier.get())
                              .get()
                              .isEqualTo(initialObj)
                              .isNotSameAs(initialObj);

                          var attempt = iteration.incrementAndGet();

                          if (attempt < 4) {
                            // retry
                            return Optional.empty();
                          }

                          return Optional.of(
                              CommitTestObj.builder()
                                  .from(initialObj)
                                  .optional("some optional")
                                  .build());
                        }))
        .withMessage(
            "CommitRetryable.attempt() must not modify the returned object when using the same ID");
  }

  @Test
  public void sameRefPointer(TestInfo testInfo) {
    var initialObj =
        persistence.write(
            CommitTestObj.builder()
                .id(persistence.generateId())
                .text("initial")
                .seq(1)
                .tail(new long[0])
                .build(),
            CommitTestObj.class);
    var referenceName = testInfo.getTestMethod().orElseThrow().getName();

    var iteration = new AtomicInteger();

    persistence.write(initialObj, CommitTestObj.class);
    persistence.createReference(referenceName, Optional.of(objRef(initialObj)));

    soft.assertThatCode(
            () ->
                persistence
                    .createCommitter(referenceName, CommitTestObj.class, CommitTestObj.class)
                    .commit(
                        (state, refObjSupplier) -> {
                          soft.assertThat(refObjSupplier.get())
                              .get()
                              .isEqualTo(initialObj)
                              .isNotSameAs(initialObj);

                          var attempt = iteration.incrementAndGet();

                          if (attempt < 4) {
                            // retry
                            return Optional.empty();
                          }

                          return Optional.of(initialObj);
                        }))
        .doesNotThrowAnyException();

    soft.assertThat(persistence.fetch(objRef(initialObj), CommitTestObj.class))
        .isEqualTo(initialObj);
  }

  static ObjRef[] withPartNum1(Collection<ObjRef> src) {
    return src.stream().map(o -> ObjRef.objRef(o.type(), o.id(), 1)).toArray(ObjRef[]::new);
  }
}
