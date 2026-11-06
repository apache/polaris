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
package org.apache.polaris.service.catalog.iceberg;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.BadRequestException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

class ReferencedByChainTest {

  private static final String SEP = "\u001f";
  private static final TableIdentifier TARGET = TableIdentifier.of("db", "t");

  private static List<TableIdentifier> parse(String referencedBy) {
    return ReferencedByChain.parse(referencedBy, TARGET, 10);
  }

  @ParameterizedTest
  @NullAndEmptySource
  void absentOrEmptyMeansNoChain(String referencedBy) {
    assertThat(parse(referencedBy)).isEmpty();
  }

  @Test
  void parsesSingleEntry() {
    assertThat(parse("ns" + SEP + "v1")).containsExactly(TableIdentifier.of("ns", "v1"));
  }

  @Test
  void parsesMultiLevelNamespacesInOrder() {
    assertThat(parse("a" + SEP + "b" + SEP + "outer,c" + SEP + "inner"))
        .containsExactly(
            TableIdentifier.of(Namespace.of("a", "b"), "outer"), TableIdentifier.of("c", "inner"));
  }

  @Test
  void decodesEncodedComma() {
    assertThat(parse("ns" + SEP + "a%2Cb,n%2Cs" + SEP + "v%2cx"))
        .containsExactly(TableIdentifier.of("ns", "a,b"), TableIdentifier.of("n,s", "v,x"));
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "noseparator",
        SEP + "v",
        "ns" + SEP,
        "a" + SEP + SEP + "v",
        "ns" + SEP + "v,",
        ",ns" + SEP + "v",
        "ns" + SEP + "v,,ns" + SEP + "w",
      })
  void rejectsMalformedChain(String referencedBy) {
    assertThatThrownBy(() -> parse(referencedBy))
        .isInstanceOf(BadRequestException.class)
        .hasMessageContaining("referenced-by");
  }

  @Test
  void acceptsChainAtMaxDepth() {
    assertThat(ReferencedByChain.parse("n" + SEP + "a,n" + SEP + "b", TARGET, 2)).hasSize(2);
  }

  @Test
  void rejectsChainDeeperThanMaxDepth() {
    assertThatThrownBy(() -> ReferencedByChain.parse("n" + SEP + "a,n" + SEP + "b", TARGET, 1))
        .isInstanceOf(BadRequestException.class)
        .hasMessageContaining("maximum is 1");
  }

  @Test
  void rejectsRepeatedEntry() {
    assertThatThrownBy(() -> parse("n" + SEP + "a,n" + SEP + "b,n" + SEP + "a"))
        .isInstanceOf(BadRequestException.class)
        .hasMessageContaining("repeats");
  }

  @Test
  void rejectsChainContainingTarget() {
    assertThatThrownBy(() -> parse("n" + SEP + "a,db" + SEP + "t"))
        .isInstanceOf(BadRequestException.class)
        .hasMessageContaining("contains the target");
  }

  @Test
  void parseWithoutTargetDoesNotValidateChain() {
    assertThat(ReferencedByChain.parse("n" + SEP + "a,n" + SEP + "a")).hasSize(2);
  }
}
