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

import java.security.Principal;
import java.util.Map;
import java.util.Set;
import org.apache.polaris.core.entity.PrincipalEntity;
import org.apache.polaris.immutables.PolarisImmutable;

/** Represents a {@link Principal} in the Polaris system. */
@PolarisImmutable
public interface PolarisPrincipal extends Principal {

  /**
   * Creates a new instance of {@link PolarisPrincipal} from the given {@link PrincipalEntity} and
   * roles.
   *
   * <p>The created principal will have the same ID and name as the {@link PrincipalEntity}, and its
   * properties will be derived from the internal properties of the entity.
   *
   * @param principalEntity the principal entity representing the user or service
   * @param roles the set of roles associated with the principal
   */
  static PolarisPrincipal of(PrincipalEntity principalEntity, Set<String> roles) {
    return of(principalEntity.getName(), principalEntity.getInternalPropertiesAsMap(), roles);
  }

  /**
   * Creates a new instance of {@link PolarisPrincipal} with the specified ID, name, roles, and
   * properties.
   *
   * @param name the name of the principal
   * @param properties additional properties associated with the principal
   * @param roles the set of roles associated with the principal
   */
  static PolarisPrincipal of(String name, Map<String, String> properties, Set<String> roles) {
    return ImmutablePolarisPrincipal.builder()
        .name(name)
        .properties(properties)
        .roles(roles)
        .build();
  }

  /**
   * Returns the set of activated principal role names.
   *
   * <p>Activated role names are the roles that were explicitly requested by the client when
   * authenticating, through JWT claims or other means. It may be a subset of the roles that the
   * principal has in the system.
   */
  Set<String> getRoles();

  /**
   * Returns the properties of this principal.
   *
   * <p>Properties are key-value pairs that provide additional information about the principal, such
   * as permissions, preferences, or other metadata.
   */
  Map<String, String> getProperties();
}
