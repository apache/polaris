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

import java.util.Set;
import org.apache.polaris.core.entity.PrincipalEntity;

/** Holds the results of request authentication. */
public interface AuthenticatedPolarisPrincipal extends java.security.Principal {

  AuthenticatedPolarisPrincipal ANONYMOUS =
      new AuthenticatedPolarisPrincipalImpl(-1, "anonymous", Set.of());

  /**
   * Principal entity ID obtained during request authentication (e.g. from the authorization token).
   *
   * <p>Negative values indicate that a principal ID was not provided in authenticated data,
   * however, other authentic information about the principal (e.g. name, roles) may still be
   * available.
   */
  long getPrincipalEntityId();

  /** A sub-set of the assigned roles that are deemed effective in requests using this principal. */
  Set<String> getActivatedPrincipalRoleNames();

  static AuthenticatedPolarisPrincipal create(long entityId, String name, Set<String> roles) {
    return new AuthenticatedPolarisPrincipalImpl(entityId, name, roles);
  }

  static AuthenticatedPolarisPrincipal fromEntity(PrincipalEntity entity) {
    return fromEntity(entity, Set.of());
  }

  static AuthenticatedPolarisPrincipal fromEntity(
      PrincipalEntity entity, Set<String> activatedPrincipalRoles) {
    return new AuthenticatedPolarisPrincipalImpl(
        entity.getId(), entity.getName(), activatedPrincipalRoles);
  }
}
