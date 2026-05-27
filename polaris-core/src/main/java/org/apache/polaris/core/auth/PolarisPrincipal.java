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
import org.apache.polaris.core.entity.PrincipalEntity;
import org.apache.polaris.immutables.PolarisImmutable;
import org.immutables.value.Value;

/** Represents a {@link Principal} in the Polaris system. */
@PolarisImmutable
public interface PolarisPrincipal extends Principal {

  /** Indicates how principal roles should be selected during role resolution. */
  enum RoleSelection {
    /** Resolve the specific role names returned by {@link PolarisPrincipal#getRoles()}. */
    SELECTED_ROLES,

    /** Resolve all roles currently granted to the principal. */
    ALL_ROLES
  }

  /**
   * Creates a new instance of {@link PolarisPrincipal} from the given {@link PrincipalEntity} and
   * selected role names.
   *
   * <p>The created principal will have the same ID and name as the {@link PrincipalEntity}, and its
   * properties will be derived from the internal properties of the entity. Role resolution will be
   * limited to the given {@code roles}; an empty set means no principal roles are selected.
   *
   * @param principalEntity the principal entity representing the user or service
   * @param roles the selected principal role names
   */
  static PolarisPrincipal of(PrincipalEntity principalEntity, Set<String> roles) {
    return of(principalEntity, roles, RoleSelection.SELECTED_ROLES);
  }

  /**
   * Creates a new instance of {@link PolarisPrincipal} from the given {@link PrincipalEntity},
   * selected role names, and role selection mode.
   *
   * <p>The created principal will have the same ID and name as the {@link PrincipalEntity}, and its
   * properties will be derived from the internal properties of the entity.
   *
   * @param principalEntity the principal entity representing the user or service
   * @param roles the selected principal role names
   * @param roleSelection how principal roles should be selected during role resolution
   */
  static PolarisPrincipal of(
      PrincipalEntity principalEntity, Set<String> roles, RoleSelection roleSelection) {
    return of(
        principalEntity.getName(),
        principalEntity.getInternalPropertiesAsMap(),
        roles,
        roleSelection,
        Optional.empty());
  }

  /**
   * Creates a new instance of {@link PolarisPrincipal} from the given {@link PrincipalEntity} and
   * selected role names.
   *
   * <p>The created principal will have the same ID and name as the {@link PrincipalEntity}, and its
   * properties will be derived from the internal properties of the entity. Role resolution will be
   * limited to the given {@code roles}; an empty set means no principal roles are selected.
   *
   * @param principalEntity the principal entity representing the user or service
   * @param roles the selected principal role names
   * @param token the access token of the current user
   */
  static PolarisPrincipal of(
      PrincipalEntity principalEntity, Set<String> roles, Optional<String> token) {
    return of(principalEntity, roles, RoleSelection.SELECTED_ROLES, token);
  }

  /**
   * Creates a new instance of {@link PolarisPrincipal} from the given {@link PrincipalEntity},
   * selected role names, role selection mode, and token.
   *
   * <p>The created principal will have the same ID and name as the {@link PrincipalEntity}, and its
   * properties will be derived from the internal properties of the entity.
   *
   * @param principalEntity the principal entity representing the user or service
   * @param roles the selected principal role names
   * @param roleSelection how principal roles should be selected during role resolution
   * @param token the access token of the current user
   */
  static PolarisPrincipal of(
      PrincipalEntity principalEntity,
      Set<String> roles,
      RoleSelection roleSelection,
      Optional<String> token) {
    return of(
        principalEntity.getName(),
        principalEntity.getInternalPropertiesAsMap(),
        roles,
        roleSelection,
        token);
  }

  /**
   * Creates a new instance of {@link PolarisPrincipal} with the specified name, properties, and
   * selected role names.
   *
   * <p>Role resolution will be limited to the given {@code roles}; an empty set means no principal
   * roles are selected.
   *
   * @param name the name of the principal
   * @param properties additional properties associated with the principal
   * @param roles the selected principal role names
   */
  static PolarisPrincipal of(String name, Map<String, String> properties, Set<String> roles) {
    return of(name, properties, roles, RoleSelection.SELECTED_ROLES);
  }

  /**
   * Creates a new instance of {@link PolarisPrincipal} with the specified name, properties,
   * selected role names, and role selection mode.
   *
   * @param name the name of the principal
   * @param properties additional properties associated with the principal
   * @param roles the selected principal role names
   * @param roleSelection how principal roles should be selected during role resolution
   */
  static PolarisPrincipal of(
      String name, Map<String, String> properties, Set<String> roles, RoleSelection roleSelection) {
    return of(name, properties, roles, roleSelection, Optional.empty());
  }

  /**
   * Creates a new instance of {@link PolarisPrincipal} with the specified name, properties,
   * selected role names, and token.
   *
   * <p>Role resolution will be limited to the given {@code roles}; an empty set means no principal
   * roles are selected.
   *
   * @param name the name of the principal
   * @param properties additional properties associated with the principal
   * @param roles the selected principal role names
   * @param token the access token of the current user
   */
  static PolarisPrincipal of(
      String name, Map<String, String> properties, Set<String> roles, Optional<String> token) {
    return of(name, properties, roles, RoleSelection.SELECTED_ROLES, token);
  }

  /**
   * Creates a principal whose role selection should include all roles currently granted to the
   * principal.
   *
   * <p>The returned principal does not carry a role-name snapshot. Callers that need a materialized
   * list for downstream context should use an overload that accepts {@code roles}.
   *
   * @param principalEntity the principal entity representing the user or service
   */
  static PolarisPrincipal ofAllRoles(PrincipalEntity principalEntity) {
    return ofAllRoles(principalEntity, Optional.empty());
  }

  /**
   * Creates a principal whose role selection should include all roles currently granted to the
   * principal.
   *
   * <p>The returned principal does not carry a role-name snapshot. Callers that need a materialized
   * list for downstream context should use an overload that accepts {@code roles}.
   *
   * @param principalEntity the principal entity representing the user or service
   * @param token the access token of the current user
   */
  static PolarisPrincipal ofAllRoles(PrincipalEntity principalEntity, Optional<String> token) {
    return ofAllRoles(principalEntity, Set.of(), token);
  }

  /**
   * Creates a principal whose role selection should include all roles currently granted to the
   * principal.
   *
   * <p>The returned principal does not carry a role-name snapshot. Callers that need a materialized
   * list for downstream context should use an overload that accepts {@code roles}.
   *
   * @param name the name of the principal
   * @param properties additional properties associated with the principal
   * @param token the access token of the current user
   */
  static PolarisPrincipal ofAllRoles(
      String name, Map<String, String> properties, Optional<String> token) {
    return ofAllRoles(name, properties, Set.of(), token);
  }

  /**
   * Creates a principal whose role selection should include all roles currently granted to the
   * principal.
   *
   * <p>The {@code roles} argument is a materialized snapshot of activated role names, as captured
   * or supplied when the principal was created, for downstream context such as request identity
   * roles, diagnostics, events, and credential vending context. It does not limit role resolution
   * when {@link #getRoleSelection()} is {@link RoleSelection#ALL_ROLES}.
   *
   * @param principalEntity the principal entity representing the user or service
   * @param roles materialized activated role names, if already known
   * @param token the access token of the current user
   */
  static PolarisPrincipal ofAllRoles(
      PrincipalEntity principalEntity, Set<String> roles, Optional<String> token) {
    return of(principalEntity, roles, RoleSelection.ALL_ROLES, token);
  }

  /**
   * Creates a principal whose role selection should include all roles currently granted to the
   * principal.
   *
   * <p>The {@code roles} argument is a materialized snapshot of activated role names, as captured
   * or supplied when the principal was created, for downstream context such as request identity
   * roles, diagnostics, events, and credential vending context. It does not limit role resolution
   * when {@link #getRoleSelection()} is {@link RoleSelection#ALL_ROLES}.
   *
   * @param name the name of the principal
   * @param properties additional properties associated with the principal
   * @param roles materialized activated role names, if already known
   * @param token the access token of the current user
   */
  static PolarisPrincipal ofAllRoles(
      String name, Map<String, String> properties, Set<String> roles, Optional<String> token) {
    return of(name, properties, roles, RoleSelection.ALL_ROLES, token);
  }

  /**
   * Creates a new instance of {@link PolarisPrincipal} with the specified name, properties,
   * selected role names, role selection mode, and token.
   *
   * <p>When {@code roleSelection} is {@link RoleSelection#SELECTED_ROLES}, role resolution will be
   * limited to the given {@code roles}; an empty set means no principal roles are selected. When
   * {@code roleSelection} is {@link RoleSelection#ALL_ROLES}, role resolution is based on the
   * principal's current grants rather than the role names returned by {@link #getRoles()}.
   *
   * @param name the name of the principal
   * @param properties additional properties associated with the principal
   * @param roles the selected principal role names or materialized role-name snapshot
   * @param roleSelection how principal roles should be selected during role resolution
   * @param token the access token of the current user
   */
  static PolarisPrincipal of(
      String name,
      Map<String, String> properties,
      Set<String> roles,
      RoleSelection roleSelection,
      Optional<String> token) {
    return ImmutablePolarisPrincipal.builder()
        .name(name)
        .properties(properties)
        .token(token)
        .roles(roles)
        .roleSelection(roleSelection)
        .build();
  }

  /**
   * Returns the set of activated principal role names.
   *
   * <p>When {@link #getRoleSelection()} is {@link RoleSelection#SELECTED_ROLES}, this set is the
   * selected role set used for role resolution. An empty set means no principal roles are selected.
   *
   * <p>When {@link #getRoleSelection()} is {@link RoleSelection#ALL_ROLES}, this set is only a
   * materialized snapshot of activated role names, as captured or supplied when the principal was
   * created, for downstream context. It does not limit role resolution.
   */
  Set<String> getRoles();

  /** Returns how principal roles should be selected during role resolution. */
  RoleSelection getRoleSelection();

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
