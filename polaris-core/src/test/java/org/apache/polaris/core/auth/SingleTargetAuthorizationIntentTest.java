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
package org.apache.polaris.core.auth;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.List;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.junit.jupiter.api.Test;

class SingleTargetAuthorizationIntentTest {

  private static final PolarisAuthorizableOperation OP = PolarisAuthorizableOperation.LOAD_TABLE;

  private static PolarisSecurable table(String name) {
    return PolarisSecurable.of(
        new PathSegment(PolarisEntityType.CATALOG, "catalog"),
        new PathSegment(PolarisEntityType.NAMESPACE, "ns"),
        new PathSegment(PolarisEntityType.TABLE_LIKE, name));
  }

  @Test
  void twoArgumentConstructorHasNoChain() {
    SingleTargetAuthorizationIntent intent = new SingleTargetAuthorizationIntent(OP, table("t"));

    assertThat(intent.referencedBy()).isEmpty();
  }

  @Test
  void keepsChainOrder() {
    List<PolarisSecurable> chain = List.of(table("outer"), table("inner"));

    SingleTargetAuthorizationIntent intent =
        new SingleTargetAuthorizationIntent(OP, table("t"), chain);

    assertThat(intent.referencedBy()).containsExactlyElementsOf(chain);
  }

  @Test
  void chainIsCopied() {
    List<PolarisSecurable> chain = new ArrayList<>(List.of(table("outer")));

    SingleTargetAuthorizationIntent intent =
        new SingleTargetAuthorizationIntent(OP, table("t"), chain);
    chain.add(table("other"));

    assertThat(intent.referencedBy()).containsExactly(table("outer"));
  }

  @Test
  void visitSecurablesOnlyVisitsTarget() {
    SingleTargetAuthorizationIntent intent =
        new SingleTargetAuthorizationIntent(OP, table("t"), List.of(table("outer")));
    List<PolarisSecurable> visited = new ArrayList<>();

    intent.visitSecurables(visited::add);

    assertThat(visited).containsExactly(table("t"));
  }

  @Test
  void rejectsNullChain() {
    assertThatThrownBy(() -> new SingleTargetAuthorizationIntent(OP, table("t"), null))
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining("referencedBy must be non-null");
  }
}
