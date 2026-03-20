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

import java.util.EnumSet;
import org.apache.polaris.core.auth.RbacOperationSemantics.ResolvedPathRooting;
import org.apache.polaris.core.entity.PolarisPrivilege;
import org.junit.jupiter.api.Test;

public class RbacOperationSemanticsTest {

  @Test
  void allOperationsHaveExplicitRbacSemantics() {
    // Ensure all PolarisAuthorizableOperation have RbacOperationSemantics defined
    for (PolarisAuthorizableOperation operation : PolarisAuthorizableOperation.values()) {
      assertThat(RbacOperationSemantics.forOperation(operation)).isNotNull();
    }
  }

  @Test
  void nullSecondaryPrivilegesNormalizeToEmpty() {
    RbacOperationSemantics semantics =
        new RbacOperationSemantics(
            EnumSet.of(PolarisPrivilege.TABLE_DROP), null, ResolvedPathRooting.ROOT);

    assertThat(semantics.secondaryPrivileges()).isEmpty();
    assertThat(semantics.rooting()).isEqualTo(ResolvedPathRooting.ROOT);
  }

  @Test
  void targetPrivilegesMustBeNonNull() {
    assertThatThrownBy(() -> new RbacOperationSemantics(null, null, ResolvedPathRooting.ROOT))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("targetPrivileges must be non-null");
  }

  @Test
  void emptyTargetPrivilegesAreRejected() {
    assertThatThrownBy(
            () ->
                new RbacOperationSemantics(
                    EnumSet.noneOf(PolarisPrivilege.class), null, ResolvedPathRooting.ROOT))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("targetPrivileges must be non-empty");
  }

  @Test
  void rootingIsRequired() {
    assertThatThrownBy(
            () -> new RbacOperationSemantics(EnumSet.of(PolarisPrivilege.TABLE_DROP), null, null))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("rooting must be non-null");
  }
}
