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

import jakarta.annotation.Nonnull;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.Collection;
import java.util.Set;

/**
 * Container holding all defined {@linkplain Privilege privileges}.
 *
 * <p>Implementation is provided as an {@link ApplicationScoped @ApplicationScoped} bean.
 */
public interface Privileges {
  /**
   * Return the {@linkplain Privilege privilege} for the given ID.
   *
   * @throws IllegalArgumentException if no privilege for the given ID exists.
   */
  Privilege byId(int id);

  /**
   * Return the {@linkplain Privilege privilege} for the given name (case-sensitive).
   *
   * @throws IllegalArgumentException if no privilege for the given name exists.
   */
  Privilege byName(@Nonnull String name);

  int idForName(@Nonnull String name);

  int idForPrivilege(@Nonnull Privilege privilege);

  PrivilegeSet nonInheritablePrivileges();

  /**
   * Returns the set of {@linkplain Privilege privilege} from the given {@linkplain PrivilegeSet
   * privilege set}, replacing all {@linkplain Privilege.IndividualPrivilege individual privileges}
   * that fully match the {@linkplain Privilege.CompositePrivilege composite privileges}. If
   * multiple composite privileges match, all of those will be returned.
   */
  Set<Privilege> collapseComposites(@Nonnull PrivilegeSet value);

  /** Informative function, returns all known {@linkplain Privilege privileges}. */
  Collection<Privilege> all();

  /** Informative function, the IDs provided all known {@linkplain Privilege privileges}. */
  Set<Integer> allIds();

  /** Informative function, returns the names of all known {@linkplain Privilege privileges}. */
  Set<String> allNames();

  PrivilegeSet.PrivilegeSetBuilder newPrivilegesSetBuilder();

  Acl.AclBuilder newAclBuilder();

  AclEntry.AclEntryBuilder newAclEntryBuilder();

  PrivilegeCheck startPrivilegeCheck(boolean anonymous, Set<String> roleIds);
}
