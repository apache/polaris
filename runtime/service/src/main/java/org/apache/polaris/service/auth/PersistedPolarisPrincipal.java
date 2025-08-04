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
package org.apache.polaris.service.auth;

import java.util.Map;
import java.util.Set;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.entity.PrincipalEntity;
import org.apache.polaris.immutables.PolarisImmutable;

/**
 * A persisted {@link PolarisPrincipal}, exposing the underlying {@link PrincipalEntity}.
 *
 * <p>Note: This class is intended for internal use within the Polaris authentication system.
 */
@PolarisImmutable
abstract class PersistedPolarisPrincipal implements PolarisPrincipal {

  /**
   * Creates a new instance of {@link PersistedPolarisPrincipal} from the given {@link
   * PrincipalEntity} and roles.
   */
  static PersistedPolarisPrincipal of(PrincipalEntity principalEntity, Set<String> principalRoles) {
    return ImmutablePersistedPolarisPrincipal.builder()
        .entity(principalEntity)
        .roles(principalRoles)
        .build();
  }

  abstract PrincipalEntity getEntity();

  @Override
  public final String getName() {
    return getEntity().getName();
  }

  @Override
  public final long getId() {
    return getEntity().getId();
  }

  @Override
  public final Map<String, String> getProperties() {
    return getEntity().getInternalPropertiesAsMap();
  }
}
