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
import java.util.Optional;
import java.util.Set;
import org.apache.polaris.immutables.PolarisImmutable;
import org.immutables.value.Value;

/** Represents a {@link Principal} in the Polaris system. */
@PolarisImmutable
public interface PolarisPrincipal extends Principal {

  /**
   * Attribute key for the principal entity attribute, of type {@link
   * org.apache.polaris.core.entity.PrincipalEntity}.
   *
   * <p>Note: callers must never assume that this attribute is present.
   */
  String PRINCIPAL_ENTITY_ATTRIBUTE_KEY = "org.apache.polaris.core.auth.PRINCIPAL_ENTITY";

  /**
   * Attribute key, of type {@link Boolean}, used to determine how authorizers should resolve
   * principal roles.
   *
   * <p>When absent or false, authorizers should resolve only the roles returned by {@link
   * #getRoles()}; if an empty set is returned, then no principal roles should be resolved.
   *
   * <p>When present and true, authorizers should resolve all roles granted to the principal at the
   * time of check, ignoring roles returned by {@link #getRoles()} (even if an empty set is
   * returned).
   *
   * <p>This attribute is generally present and true when the principal presented a credential
   * including the pseudo-role {@code PRINCIPAL_ROLE:ALL}.
   *
   * <p>Note: Callers must not assume that this attribute is always present.
   */
  String PRINCIPAL_ROLE_ALL_ATTRIBUTE_KEY = "org.apache.polaris.core.auth.PRINCIPAL_ROLE:ALL";

  /**
   * Attribute key used to store or retrieve a JSON Web Token (JWT) associated with the {@link
   * PolarisPrincipal}, of type {@link String}.
   *
   * <p>The associated value is expected to represent the JWT provided during authentication and may
   * be used for further validation or for deriving additional claims.
   *
   * <p>Note: callers must never assume that this attribute is present.
   */
  String JWT_ATTRIBUTE_KEY = "org.apache.polaris.core.auth.JWT";

  /**
   * Creates a new instance of {@link PolarisPrincipal} with the specified name, roles, and
   * attributes.
   *
   * @param name the name of the principal
   * @param attributes the attributes of the principal
   * @param roles the set of roles associated with the principal
   */
  static PolarisPrincipal of(String name, Map<String, Object> attributes, Set<String> roles) {
    return ImmutablePolarisPrincipal.builder()
        .name(name)
        .attributes(attributes)
        .roles(roles)
        .build();
  }

  /** Returns the principal attributes. */
  @Value.Redacted
  Map<String, Object> getAttributes();

  /** Returns the attribute value associated with the given key, if any. */
  default <T> Optional<T> getAttribute(String key, Class<T> type) {
    Object value = getAttributes().get(key);
    return type.isInstance(value) ? Optional.of(type.cast(value)) : Optional.empty();
  }

  /**
   * Returns the set of activated principal role names.
   *
   * <p>Activated role names are the roles that were explicitly requested by the client when
   * authenticating, through JWT claims or other means. It may be a subset of the roles that the
   * principal has in the system.
   */
  Set<String> getRoles();
}
