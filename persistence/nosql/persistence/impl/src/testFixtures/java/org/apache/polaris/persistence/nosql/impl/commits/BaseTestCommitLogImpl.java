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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.stream.IntStream;
import org.apache.polaris.persistence.nosql.api.Persistence;
import org.apache.polaris.persistence.nosql.testextension.PersistenceTestExtension;
import org.apache.polaris.persistence.nosql.testextension.PolarisPersistence;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

@ExtendWith({PersistenceTestExtension.class, SoftAssertionsExtension.class})
public abstract class BaseTestCommitLogImpl {
  @InjectSoftAssertions protected SoftAssertions soft;
  @PolarisPersistence protected Persistence persistence;

  @ParameterizedTest
  @ValueSource(ints = {0, 1, 3, 19, 20, 21, 39, 40, 41, 255})
  public void commitLogs(int numCommits, TestInfo testInfo) throws Exception {
    var refName = testInfo.getTestMethod().orElseThrow().getName();

    persistence.createReference(refName, Optional.empty());

    var committer = persistence.createCommitter(refName, SimpleCommitTestObj.class, String.class);
    for (int i = 0; i < numCommits; i++) {
      var payload = "commit #" + i;
      committer.commit(
          (state, refObjSupplier) -> {
            var refObj = refObjSupplier.get();
            return state.commitResult(
                "foo", ImmutableSimpleCommitTestObj.builder().payload(payload), refObj);
          });
    }

    // Commit log in "reversed" (most recent commit last)
    var commits = persistence.commits();
    var expectedPayloads = IntStream.range(0, numCommits).mapToObj(i -> "commit #" + i).toList();
    soft.assertThatIterator(commits.commitLogReversed(refName, 0L, SimpleCommitTestObj.class))
        .toIterable()
        .extracting(SimpleCommitTestObj::payload)
        .containsExactlyElementsOf(expectedPayloads);

    // Commit log in "natural" (most recent commit first)
    Collections.reverse(expectedPayloads);
    soft.assertThatIterator(
            commits.commitLog(refName, OptionalLong.empty(), SimpleCommitTestObj.class))
        .toIterable()
        .extracting(SimpleCommitTestObj::payload)
        .containsExactlyElementsOf(expectedPayloads);
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 3, 21, 41})
  public void commitLogOffsets(int offsetIndex, TestInfo testInfo) throws Exception {
    var refName = testInfo.getTestMethod().orElseThrow().getName() + "-" + offsetIndex;
    var numCommits = 50;

    persistence.createReference(refName, Optional.empty());

    var committer = persistence.createCommitter(refName, SimpleCommitTestObj.class, String.class);
    for (int i = 0; i < numCommits; i++) {
      var payload = "commit #" + i;
      committer.commit(
          (state, refObjSupplier) ->
              state.commitResult(
                  "foo",
                  ImmutableSimpleCommitTestObj.builder().payload(payload),
                  refObjSupplier.get()));
    }

    var commits = persistence.commits();
    var natural =
        toList(commits.commitLog(refName, OptionalLong.empty(), SimpleCommitTestObj.class));
    var chronological = toList(commits.commitLogReversed(refName, 0L, SimpleCommitTestObj.class));
    Collections.reverse(chronological);
    soft.assertThat(chronological).containsExactlyElementsOf(natural);

    var offsetCommit = natural.get(offsetIndex);
    soft.assertThat(
            toList(
                commits.commitLog(
                    refName, OptionalLong.of(offsetCommit.id()), SimpleCommitTestObj.class)))
        .containsExactlyElementsOf(natural.subList(offsetIndex, natural.size()));

    Collections.reverse(chronological);
    var chronologicalOffsetCommit = chronological.get(offsetIndex);
    soft.assertThat(
            toList(
                commits.commitLogReversed(
                    refName, chronologicalOffsetCommit.id(), SimpleCommitTestObj.class)))
        .containsExactlyElementsOf(chronological.subList(offsetIndex + 1, chronological.size()));

    soft.assertThat(
            toList(
                commits.commitLog(
                    refName, OptionalLong.of(persistence.generateId()), SimpleCommitTestObj.class)))
        .containsExactly(natural.getLast());
    soft.assertThat(
            toList(
                commits.commitLogReversed(
                    refName, persistence.generateId(), SimpleCommitTestObj.class)))
        .containsExactlyElementsOf(chronological);
  }

  private static List<SimpleCommitTestObj> toList(Iterator<SimpleCommitTestObj> iterator) {
    var result = new ArrayList<SimpleCommitTestObj>();
    iterator.forEachRemaining(result::add);
    return result;
  }
}
