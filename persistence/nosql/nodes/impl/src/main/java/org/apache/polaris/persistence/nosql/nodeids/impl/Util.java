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
package org.apache.polaris.persistence.nosql.nodeids.impl;

import com.google.common.annotations.VisibleForTesting;
import java.util.Optional;
import org.apache.polaris.ids.api.IdGeneratorSpec;
import org.apache.polaris.ids.api.ImmutableBuildableIdGeneratorSpec;
import org.apache.polaris.persistence.nosql.nodeids.spi.NodeManagementState;

public final class Util {
  private Util() {}

  @VisibleForTesting
  public static IdGeneratorSpec idgenSpecFromManagementState(
      Optional<NodeManagementState> existingNodeManagementState) {
    return existingNodeManagementState
        .orElseThrow()
        .idGeneratorSpec()
        .orElse(ImmutableBuildableIdGeneratorSpec.builder().build());
  }
}
