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
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.polaris.core.entity.PrincipalEntity;
import org.apache.polaris.immutables.PolarisImmutable;
import org.immutables.value.Value;

/** Represents a {@link Principal} in the Polaris system. */
@PolarisImmutable
public interface PolarisPrincipal extends Principal {

  /**
   * Creates a new instance of {@link PolarisPrincipal} from the given {@link PrincipalEntity} and
   * roles.
   *
   * <p>The created principal will have the same name as the {@link PrincipalEntity}, and its
   * properties will be the merger of the entity's user-defined properties and its internal
   * properties. If a key appears in both maps, the internal-property value wins, so that
   * system-managed values (for example the {@code client_id}) cannot be shadowed by user input.
   *
   * @param principalEntity the principal entity representing the user or service
   * @param roles the set of roles associated with the principal
   */
  static PolarisPrincipal of(PrincipalEntity principalEntity, Set<String> roles) {
    return of(principalEntity, roles, Optional.empty());
  }

  /**
   * Creates a new instance of {@link PolarisPrincipal} from the given {@link PrincipalEntity} and
   * roles.
   *
   * <p>The created principal will have the same name as the {@link PrincipalEntity}, and its
   * properties will be the merger of the entity's user-defined properties and its internal
   * properties. If a key appears in both maps, the internal-property value wins, so that
   * system-managed values (for example the {@code client_id}) cannot be shadowed by user input.
   *
   * @param principalEntity the principal entity representing the user or service
   * @param roles the set of roles associated with the principal
   * @param token the access token of the current user
   */
  static PolarisPrincipal of(
      PrincipalEntity principalEntity, Set<String> roles, Optional<String> token) {
    return of(principalEntity.getName(), mergeEntityProperties(principalEntity), roles, token);
  }

  /**
   * Merges the user-defined and internal property maps of a {@link PrincipalEntity}. On key
   * collision the internal value wins, so that system-managed properties (for example {@code
   * client_id}) cannot be shadowed by user-supplied properties.
   */
  private static Map<String, String> mergeEntityProperties(PrincipalEntity principalEntity) {
    Map<String, String> merged = new HashMap<>(principalEntity.getPropertiesAsMap());
    merged.putAll(principalEntity.getInternalPropertiesAsMap());
    return merged;
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
    return of(name, properties, roles, Optional.empty());
  }

  /**
   * Creates a new instance of {@link PolarisPrincipal} with the specified ID, name, roles, and
   * properties.
   *
   * @param name the name of the principal
   * @param properties additional properties associated with the principal
   * @param roles the set of roles associated with the principal
   * @param token the access token of the current user
   */
  static PolarisPrincipal of(
      String name, Map<String, String> properties, Set<String> roles, Optional<String> token) {
    return ImmutablePolarisPrincipal.builder()
        .name(name)
        .properties(properties)
        .token(token)
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

  /** Optionally returns the access token of the current user. */
  @Value.Redacted
  Optional<String> getToken();
}
