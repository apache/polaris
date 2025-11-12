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
package org.apache.polaris.persistence.nosql.authz.impl;

import jakarta.annotation.Nonnull;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.polaris.persistence.nosql.authz.spi.PrivilegesMapping;
import org.apache.polaris.persistence.nosql.authz.spi.PrivilegesRepository;

class PrivilegesTestRepository implements PrivilegesRepository {
  private final AtomicReference<PrivilegesMapping> current =
      new AtomicReference<>(PrivilegesMapping.builder().build());

  @Override
  public boolean updatePrivilegesMapping(
      @Nonnull PrivilegesMapping expectedState, @Nonnull PrivilegesMapping newState) {
    var v = current.updateAndGet(curr -> curr.equals(expectedState) ? newState : curr);
    return v.equals(newState);
  }

  @Override
  @Nonnull
  public PrivilegesMapping fetchPrivilegesMapping() {
    return Optional.ofNullable(current.get()).orElse(PrivilegesMapping.EMPTY);
  }
}
