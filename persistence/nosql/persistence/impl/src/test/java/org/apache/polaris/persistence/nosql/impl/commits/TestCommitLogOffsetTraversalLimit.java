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

import static java.util.function.Function.identity;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.UUID;
import org.apache.polaris.ids.api.IdGenerator;
import org.apache.polaris.ids.api.MonotonicClock;
import org.apache.polaris.persistence.nosql.api.Persistence;
import org.apache.polaris.persistence.nosql.api.PersistenceParams;
import org.apache.polaris.persistence.nosql.api.backend.Backend;
import org.apache.polaris.persistence.nosql.api.exceptions.CommitOffsetTraversalLimitExceededException;
import org.apache.polaris.persistence.nosql.testextension.BackendSpec;
import org.apache.polaris.persistence.nosql.testextension.PersistenceTestExtension;
import org.apache.polaris.persistence.nosql.testextension.PolarisPersistence;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@BackendSpec
@ExtendWith(PersistenceTestExtension.class)
public class TestCommitLogOffsetTraversalLimit {

  @PolarisPersistence Backend backend;
  @PolarisPersistence MonotonicClock clock;
  @PolarisPersistence IdGenerator idGenerator;

  @Test
  public void commitLog_offsetLookup_failsFastWhenLimitExceeded() throws Exception {
    var persistence =
        backend.newPersistence(
            identity(),
            PersistenceParams.BuildablePersistenceParams.builder()
                .commitOffsetLookupMaxTraversal(3)
                .build(),
            UUID.randomUUID().toString(),
            clock,
            idGenerator);

    var refName = "ref-" + UUID.randomUUID();
    persistence.createReference(refName, Optional.empty());

    var committer = persistence.createCommitter(refName, SimpleCommitTestObj.class, String.class);
    for (int i = 0; i < 10; i++) {
      var payload = "commit #" + i;
      committer.commit(
          (state, refObjSupplier) ->
              state.commitResult(
                  "ok",
                  ImmutableSimpleCommitTestObj.builder().payload(payload),
                  refObjSupplier.get()));
    }

    // Offset value that will never be found and requires traversal to resolve.
    assertThatThrownBy(
            () ->
                persistence
                    .commits()
                    .commitLog(
                        refName,
                        OptionalLong.of(Long.MIN_VALUE + 7),
                        SimpleCommitTestObj.class))
        .isInstanceOf(CommitOffsetTraversalLimitExceededException.class)
        .hasMessageContaining("Exceeded commit offset traversal limit");
  }

  @Test
  public void commitLogReversed_offsetLookup_failsFastWhenLimitExceeded() throws Exception {
    Persistence persistence =
        backend.newPersistence(
            identity(),
            PersistenceParams.BuildablePersistenceParams.builder()
                .commitOffsetLookupMaxTraversal(3)
                .build(),
            UUID.randomUUID().toString(),
            clock,
            idGenerator);

    var refName = "ref-" + UUID.randomUUID();
    persistence.createReference(refName, Optional.empty());

    var committer = persistence.createCommitter(refName, SimpleCommitTestObj.class, String.class);
    for (int i = 0; i < 10; i++) {
      var payload = "commit #" + i;
      committer.commit(
          (state, refObjSupplier) ->
              state.commitResult(
                  "ok",
                  ImmutableSimpleCommitTestObj.builder().payload(payload),
                  refObjSupplier.get()));
    }

    assertThatThrownBy(
            () ->
                persistence
                    .commits()
                    .commitLogReversed(refName, Long.MIN_VALUE + 7, SimpleCommitTestObj.class))
        .isInstanceOf(CommitOffsetTraversalLimitExceededException.class)
        .hasMessageContaining("Exceeded commit offset traversal limit");
  }

  @Test
  public void commitLog_offsetLookup_succeedsWithinLimit() throws Exception {
    var persistence =
        backend.newPersistence(
            identity(),
            PersistenceParams.BuildablePersistenceParams.builder()
                .commitOffsetLookupMaxTraversal(100)
                .build(),
            UUID.randomUUID().toString(),
            clock,
            idGenerator);

    var refName = "ref-" + UUID.randomUUID();
    persistence.createReference(refName, Optional.empty());

    var committer = persistence.createCommitter(refName, SimpleCommitTestObj.class, String.class);
    for (int i = 0; i < 10; i++) {
      var payload = "commit #" + i;
      committer.commit(
          (state, refObjSupplier) ->
              state.commitResult(
                  "ok",
                  ImmutableSimpleCommitTestObj.builder().payload(payload),
                  refObjSupplier.get()));
    }

    var all = new ArrayList<SimpleCommitTestObj>();
    persistence
        .commits()
        .commitLog(refName, OptionalLong.empty(), SimpleCommitTestObj.class)
        .forEachRemaining(all::add);

    var target = all.get(5);
    var it =
        persistence
            .commits()
            .commitLog(refName, OptionalLong.of(target.id()), SimpleCommitTestObj.class);
    var first = it.next();
    assertThat(first.id()).isEqualTo(target.id());
    assertThat(first.payload()).isEqualTo(target.payload());
  }

  @Test
  public void commitLogReversed_offsetLookup_succeedsWithinLimit() throws Exception {
    var persistence =
        backend.newPersistence(
            identity(),
            PersistenceParams.BuildablePersistenceParams.builder()
                .commitOffsetLookupMaxTraversal(100)
                .build(),
            UUID.randomUUID().toString(),
            clock,
            idGenerator);

    var refName = "ref-" + UUID.randomUUID();
    persistence.createReference(refName, Optional.empty());

    var committer = persistence.createCommitter(refName, SimpleCommitTestObj.class, String.class);
    for (int i = 0; i < 10; i++) {
      var payload = "commit #" + i;
      committer.commit(
          (state, refObjSupplier) ->
              state.commitResult(
                  "ok",
                  ImmutableSimpleCommitTestObj.builder().payload(payload),
                  refObjSupplier.get()));
    }

    // Build the chronological log (oldest -> newest) with a non-existent offset sentinel.
    var chronological = new ArrayList<SimpleCommitTestObj>();
    persistence
        .commits()
        .commitLogReversed(refName, 0L, SimpleCommitTestObj.class)
        .forEachRemaining(chronological::add);

    // Pick an offset somewhere in the first half; expect everything after it (newer commits).
    var offsetCommit = chronological.get(3);
    var expected =
        chronological.subList(4, chronological.size()).stream()
            .map(SimpleCommitTestObj::id)
            .toList();

    var actual = new ArrayList<Long>();
    persistence
        .commits()
        .commitLogReversed(refName, offsetCommit.id(), SimpleCommitTestObj.class)
        .forEachRemaining(c -> actual.add(c.id()));

    assertThat(actual).containsExactlyElementsOf(expected);
  }
}

