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
package org.apache.polaris.persistence.nosql.authz.api;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.Set;
import org.apache.polaris.immutables.PolarisImmutable;
import org.immutables.value.Value;

/**
 * Represents an individual or composite <em>privilege</em>.
 *
 * <p>Composite privileges consist of multiple individual privileges.
 *
 * <p>External representations, for example, when serialized as JSON, prefer composite privileges
 * over individual ones. This means that if a {@linkplain PrivilegeSet privilege set} contains all
 * privileges included in a composite privilege, only the composite privilege is serialized. If
 * multiple composite privileges match, all matching ones are serialized.
 */
public interface Privilege {
  String name();

  Set<IndividualPrivilege> resolved();

  default boolean mustMatchAll() {
    return true;
  }

  interface IndividualPrivilege extends Privilege {
    @Override
    @Value.Auxiliary
    default Set<IndividualPrivilege> resolved() {
      return Set.of(this);
    }
  }

  /**
   * Inheritable privileges apply to the checked entity and its child entities, if applied to the
   * entity's ACL or any of its parents' ACLs.
   */
  @PolarisImmutable
  interface InheritablePrivilege extends IndividualPrivilege {
    @Override
    @Value.Parameter
    String name();

    static IndividualPrivilege inheritablePrivilege(String name) {
      return ImmutableInheritablePrivilege.of(name);
    }
  }

  /**
   * Non-inheritable privileges apply only to the checked entity if those are present in the
   * entity's ACL. Non-inheritable privileges that are present on an entity's parent are ignored
   * during access checks.
   */
  @PolarisImmutable
  interface NonInheritablePrivilege extends IndividualPrivilege {
    @Override
    @Value.Parameter
    String name();

    static NonInheritablePrivilege nonInheritablePrivilege(String name) {
      return ImmutableNonInheritablePrivilege.of(name);
    }
  }

  /**
   * A composite privilege represents a group of {@linkplain IndividualPrivilege individual
   * privileges}.
   *
   * <p>Access checks for a <em>composite</em> privilege only succeed if <em>all</em> individual
   * privileges match.
   *
   * @see AlternativePrivilege
   */
  @PolarisImmutable
  interface CompositePrivilege extends Privilege {
    @Value.Parameter(order = 1)
    @Override
    String name();

    @Override
    @Value.Parameter(order = 2)
    Set<IndividualPrivilege> resolved();

    @Value.Check
    default void check() {
      checkArgument(!resolved().isEmpty(), "Must have at least one individual privilege");
    }

    static CompositePrivilege compositePrivilege(
        String name, Iterable<IndividualPrivilege> privileges) {
      return ImmutableCompositePrivilege.of(name, privileges);
    }

    static CompositePrivilege compositePrivilege(String name, IndividualPrivilege... privileges) {
      return ImmutableCompositePrivilege.of(name, Set.of(privileges));
    }
  }

  /**
   * An "alternative privilege" represents a group of {@linkplain IndividualPrivilege individual
   * privileges}.
   *
   * <p>Access checks for a <em>alternative</em> privilege succeed if <em>any</em> individual
   * privileges of the alternative privilege matches.
   *
   * @see CompositePrivilege
   */
  @PolarisImmutable
  interface AlternativePrivilege extends Privilege {
    @Value.Parameter(order = 1)
    @Override
    String name();

    @Override
    @Value.Parameter(order = 2)
    Set<IndividualPrivilege> resolved();

    @Override
    default boolean mustMatchAll() {
      return false;
    }

    @Value.Check
    default void check() {
      checkArgument(!resolved().isEmpty(), "Must have at least one individual privilege");
    }

    static AlternativePrivilege alternativePrivilege(
        String name, Iterable<IndividualPrivilege> privileges) {
      return ImmutableAlternativePrivilege.of(name, privileges);
    }

    static AlternativePrivilege alternativePrivilege(
        String name, IndividualPrivilege... privileges) {
      return ImmutableAlternativePrivilege.of(name, Set.of(privileges));
    }
  }
}
