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
package org.apache.polaris.service.lineage;

import static org.apache.polaris.service.lineage.LineageTestEvents.datasetEvent;
import static org.apache.polaris.service.lineage.LineageTestEvents.input;
import static org.apache.polaris.service.lineage.LineageTestEvents.jobEvent;
import static org.apache.polaris.service.lineage.LineageTestEvents.output;
import static org.apache.polaris.service.lineage.LineageTestEvents.runEvent;
import static org.apache.polaris.service.lineage.LineageTestEvents.staticDataset;
import static org.assertj.core.api.Assertions.assertThat;

import io.openlineage.server.OpenLineage;
import java.net.URI;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.polaris.service.lineage.LineageDatasetKey.Role;
import org.junit.jupiter.api.Test;

/** Tests for {@link LineageDatasetExtractor}. */
class LineageDatasetExtractorTest {

  @Test
  void extractsInputsAndOutputsFromARunEvent() {
    List<LineageDatasetKey> keys =
        LineageDatasetExtractor.extract(
            runEvent(
                List.of(input("ns", "cat.s.a"), input("ns", "cat.s.b")),
                List.of(output("ns", "cat.s.c"))));

    assertThat(keys)
        .containsExactly(
            new LineageDatasetKey(Role.INPUT, "ns", "cat.s.a"),
            new LineageDatasetKey(Role.INPUT, "ns", "cat.s.b"),
            new LineageDatasetKey(Role.OUTPUT, "ns", "cat.s.c"));
  }

  @Test
  void extractsInputsAndOutputsFromAJobEvent() {
    List<LineageDatasetKey> keys =
        LineageDatasetExtractor.extract(
            jobEvent(List.of(input("ns", "cat.s.a")), List.of(output("ns", "cat.s.b"))));

    assertThat(keys)
        .containsExactly(
            new LineageDatasetKey(Role.INPUT, "ns", "cat.s.a"),
            new LineageDatasetKey(Role.OUTPUT, "ns", "cat.s.b"));
  }

  @Test
  void extractsTheSingleStandaloneDatasetFromADatasetEvent() {
    List<LineageDatasetKey> keys =
        LineageDatasetExtractor.extract(datasetEvent(staticDataset("ns", "cat.s.a")));

    assertThat(keys).containsExactly(new LineageDatasetKey(Role.STANDALONE, "ns", "cat.s.a"));
  }

  @Test
  void emptyInputAndOutputListsExtractNothing() {
    assertThat(LineageDatasetExtractor.extract(runEvent(List.of(), List.of()))).isEmpty();
    assertThat(LineageDatasetExtractor.extract(jobEvent(List.of(), List.of()))).isEmpty();
  }

  @Test
  void nullInputAndOutputListsExtractNothing() {
    assertThat(LineageDatasetExtractor.extract(runEvent(null, null))).isEmpty();
    assertThat(LineageDatasetExtractor.extract(jobEvent(null, null))).isEmpty();
  }

  @Test
  void deduplicatesRepeatedOccurrencesOfTheSameDatasetInOneRole() {
    List<LineageDatasetKey> keys =
        LineageDatasetExtractor.extract(
            runEvent(
                List.of(input("ns", "cat.s.a"), input("ns", "cat.s.a")),
                List.of(output("ns", "cat.s.b"), output("ns", "cat.s.b"))));

    assertThat(keys)
        .containsExactly(
            new LineageDatasetKey(Role.INPUT, "ns", "cat.s.a"),
            new LineageDatasetKey(Role.OUTPUT, "ns", "cat.s.b"));
  }

  @Test
  void keepsBothOccurrencesWhenOneTableIsBothInputAndOutput() {
    // A MERGE INTO reads and writes the same table. The two occurrences require different
    // privileges, so collapsing them across roles would silently drop one requirement.
    List<LineageDatasetKey> keys =
        LineageDatasetExtractor.extract(
            runEvent(List.of(input("ns", "cat.s.a")), List.of(output("ns", "cat.s.a"))));

    assertThat(keys)
        .containsExactly(
            new LineageDatasetKey(Role.INPUT, "ns", "cat.s.a"),
            new LineageDatasetKey(Role.OUTPUT, "ns", "cat.s.a"));
  }

  @Test
  void toleratesNullDatasetEntriesAndANullStandaloneDataset() {
    assertThat(
            LineageDatasetExtractor.extract(
                runEvent(Arrays.asList((OpenLineage.InputDataset) null), List.of())))
        .containsExactly(new LineageDatasetKey(Role.INPUT, null, null));
    assertThat(LineageDatasetExtractor.extract(datasetEvent(null)))
        .containsExactly(new LineageDatasetKey(Role.STANDALONE, null, null));
  }

  @Test
  void datasetsWithNullNamespaceOrNameAreStillEnumerated() {
    // They have to be: the identity predicate classifies them, and skipping them here would mean a
    // dataset the authorization pass never sees.
    assertThat(
            LineageDatasetExtractor.extract(runEvent(List.of(input(null, "cat.s.a")), List.of())))
        .containsExactly(new LineageDatasetKey(Role.INPUT, null, "cat.s.a"));
    assertThat(LineageDatasetExtractor.extract(runEvent(List.of(input("ns", null)), List.of())))
        .containsExactly(new LineageDatasetKey(Role.INPUT, "ns", null));
  }

  @Test
  void theThreeSpecEventShapesAreEnumerable() {
    assertThat(LineageDatasetExtractor.isEnumerable(runEvent(List.of(), List.of()))).isTrue();
    assertThat(LineageDatasetExtractor.isEnumerable(jobEvent(List.of(), List.of()))).isTrue();
    assertThat(LineageDatasetExtractor.isEnumerable(datasetEvent(staticDataset("ns", "n"))))
        .isTrue();
  }

  @Test
  void nullAndUnknownEventShapesAreNotEnumerable() {
    // Not the same as "has no datasets": an unrecognized shape may carry dataset references this
    // extractor cannot see, so it must not be treated as nothing-to-check.
    assertThat(LineageDatasetExtractor.isEnumerable(null)).isFalse();
    assertThat(LineageDatasetExtractor.isEnumerable(new UnknownEvent())).isFalse();
    assertThat(LineageDatasetExtractor.extract(new UnknownEvent())).isEmpty();
  }

  /** An event shape outside the three the OpenLineage spec defines. */
  private static final class UnknownEvent implements OpenLineage.BaseEvent {
    @Override
    public ZonedDateTime getEventTime() {
      return LineageTestEvents.EVENT_TIME;
    }

    @Override
    public URI getProducer() {
      return LineageTestEvents.PRODUCER;
    }

    @Override
    public URI getSchemaURL() {
      return LineageTestEvents.SCHEMA_URL;
    }

    @Override
    public Map<String, Object> getAdditionalProperties() {
      return Map.of();
    }
  }
}
