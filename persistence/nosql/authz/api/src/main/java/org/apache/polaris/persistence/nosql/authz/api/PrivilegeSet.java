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

import static org.apache.polaris.persistence.nosql.authz.api.Constants.EMPTY_PRIVILEGE_SET;

import com.google.errorprone.annotations.CanIgnoreReturnValue;
import jakarta.annotation.Nonnull;
import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

/**
 * Represents a set of <em>individual privileges</em>.
 *
 * <p>External representations, for example, when serialized as JSON, prefer composite privileges
 * over individual ones. This means that if a {@linkplain PrivilegeSet privilege set} contains all
 * privileges included in a composite privilege, only the name of the composite privilege is
 * serialized. If multiple composite privileges match, all matching ones are serialized.
 *
 * <p>This {@linkplain Set set} of {@linkplain Privilege privileges} natively represents only
 * individual privileges. To "collapse" those into composite privileges, use {@link
 * #collapseComposites(Privileges)}.
 *
 * <p>Composite privileges can however be used as arguments to all {@code contains()} functions and
 * to the {@code add()}/{@code remove()} builder methods.
 *
 * <p>Do <em>not</em> use a {@link PrivilegeSet} when the special meaning of composite privileges
 * needs to be retained, especially during access checks.
 */
public interface PrivilegeSet extends Set<Privilege> {

  static PrivilegeSet emptyPrivilegeSet() {
    return EMPTY_PRIVILEGE_SET;
  }

  boolean contains(Privilege privilege);

  Iterator<Privilege> iterator(Privileges privileges);

  @Override
  boolean isEmpty();

  byte[] toByteArray();

  default Set<Privilege> collapseComposites(Privileges privileges) {
    return privileges.collapseComposites(this);
  }

  /**
   * Checks whether the given {@link Privilege} is fully contained in this privilege set.
   *
   * <p>For {@linkplain Privilege.CompositePrivilege composite privileges}, returns {@code true} if
   * and only if all the individual privileges of a composite privilege are contained in this set.
   */
  @Override
  boolean contains(Object o);

  /**
   * Checks whether all of given {@link Privilege privileges} is contained in this privilege set.
   *
   * <p>For {@linkplain Privilege.CompositePrivilege composite privileges}, returns {@code true} if
   * and only if all the individual privileges of a composite privilege are contained in this set.
   */
  @Override
  boolean containsAll(@Nonnull Collection<?> c);

  /**
   * Checks whether any of given {@link Privilege privileges} is contained in this privilege set.
   *
   * <p>For {@linkplain Privilege.CompositePrivilege composite privileges}, returns {@code true} if
   * and only if all the individual privileges of a composite privilege are contained in this set.
   */
  boolean containsAny(Iterable<? extends Privilege> privilege);

  interface PrivilegeSetBuilder {
    @CanIgnoreReturnValue
    PrivilegeSetBuilder addPrivileges(@Nonnull Iterable<? extends Privilege> privileges);

    @CanIgnoreReturnValue
    PrivilegeSetBuilder addPrivileges(@Nonnull PrivilegeSet privilegeSet);

    @CanIgnoreReturnValue
    PrivilegeSetBuilder addPrivileges(@Nonnull Privilege... privileges);

    @CanIgnoreReturnValue
    PrivilegeSetBuilder addPrivilege(@Nonnull Privilege privilege);

    @CanIgnoreReturnValue
    PrivilegeSetBuilder removePrivileges(@Nonnull Iterable<? extends Privilege> privileges);

    @CanIgnoreReturnValue
    PrivilegeSetBuilder removePrivileges(@Nonnull PrivilegeSet privilegeSet);

    @CanIgnoreReturnValue
    PrivilegeSetBuilder removePrivileges(@Nonnull Privilege... privileges);

    @CanIgnoreReturnValue
    PrivilegeSetBuilder removePrivilege(@Nonnull Privilege privilege);

    PrivilegeSet build();
  }
}
