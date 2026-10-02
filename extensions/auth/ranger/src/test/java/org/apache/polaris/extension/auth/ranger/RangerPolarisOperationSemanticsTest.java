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

package org.apache.polaris.extension.auth.ranger;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.EnumSet;
import java.util.Map;
import org.apache.polaris.core.auth.PolarisAuthorizableOperation;
import org.junit.jupiter.api.Test;

public class RangerPolarisOperationSemanticsTest {

  /**
   * The lineage operations and the Ranger action each must map to. Kept as an explicit table so
   * that renaming an action string -- which silently invalidates every grant in a deployment's
   * Ranger service definition -- fails here rather than in production.
   */
  private static final Map<PolarisAuthorizableOperation, String> EXPECTED_LINEAGE_ACTIONS =
      Map.of(
          PolarisAuthorizableOperation.QUERY_LINEAGE,
          "lineage-query",
          PolarisAuthorizableOperation.INGEST_LINEAGE,
          "lineage-ingest",
          // Deliberately the pre-existing table-read action rather than a lineage-specific one, so
          // that an existing table-read grant is sufficient to cite a table as a lineage input --
          // matching RbacOperationSemantics, which requires TABLE_READ_PROPERTIES. A dedicated
          // access type here would make Ranger stricter than the default authorizer and force a
          // new policy for every ETL job.
          PolarisAuthorizableOperation.REFERENCE_LINEAGE_INPUT_TABLE,
          "table-properties-read");

  /**
   * {@link RangerPolarisAuthorizer} fails closed: an operation with no entry in {@link
   * RangerPolarisOperationSemantics} is denied outright. Adding a lineage operation to the core
   * enum without a Ranger mapping would therefore deny it for every Ranger deployment, with a
   * message indistinguishable from a genuine permissions denial. Assert coverage over the whole
   * lineage family rather than only the three operations that exist today.
   */
  @Test
  void everyLineageOperationHasRangerSemantics() {
    EnumSet<PolarisAuthorizableOperation> lineageOperations =
        EnumSet.allOf(PolarisAuthorizableOperation.class);
    lineageOperations.removeIf(operation -> !operation.name().contains("LINEAGE"));

    assertThat(lineageOperations).isNotEmpty();
    assertThat(lineageOperations)
        .allSatisfy(
            operation ->
                assertThat(RangerPolarisOperationSemantics.forOperation(operation))
                    .as(
                        "%s has no Ranger action mapping and would be denied as unsupported",
                        operation.name())
                    .isNotNull());
  }

  @Test
  void lineageOperationsMapToExpectedActionsRootedAtRoot() {
    EXPECTED_LINEAGE_ACTIONS.forEach(
        (operation, expectedAction) -> {
          RangerPolarisOperationSemantics semantics =
              RangerPolarisOperationSemantics.forOperation(operation);

          assertThat(semantics).as("%s", operation.name()).isNotNull();
          assertThat(semantics.targetPrivileges())
              .as("%s target privileges", operation.name())
              .containsExactly(expectedAction);
          assertThat(semantics.secondaryPrivileges())
              .as("%s secondary privileges", operation.name())
              .isEmpty();
          // Must match how RbacOperationSemantics registers these operations, otherwise the two
          // authorizers disagree about which securable is checked.
          assertThat(semantics.rooting())
              .as("%s rooting", operation.name())
              .isEqualTo(RangerPolarisOperationSemantics.ResolvedPathRooting.ROOT);
        });
  }

  /** The lineage operations are the complete set the action table above is expected to cover. */
  @Test
  void expectedLineageActionTableCoversEveryLineageOperation() {
    EnumSet<PolarisAuthorizableOperation> lineageOperations =
        EnumSet.allOf(PolarisAuthorizableOperation.class);
    lineageOperations.removeIf(operation -> !operation.name().contains("LINEAGE"));

    assertThat(EXPECTED_LINEAGE_ACTIONS.keySet())
        .containsExactlyInAnyOrderElementsOf(lineageOperations);
  }
}
