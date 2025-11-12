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

public interface PrivilegeCheck {
  /**
   * Retrieve the effective privileges, which is the set of all granted privileges minus the set of
   * all restricted privileges, for the given ACL and all its {@linkplain AclChain#parent() parent
   * ACLs}.
   *
   * <p>The set of granted privileges contains all privileges that are {@linkplain
   * AclEntry#granted() granted} to any of the role IDs for this {@linkplain PrivilegeCheck
   * privilege check instance}. A privilege is granted if it is granted to any role in the given ACL
   * or any of its parents. See note on non-inheritable privileges below.
   *
   * <p>The set of restricted privileges contains all privileges that are {@linkplain
   * AclEntry#restricted() restricted} for any of the role IDs for this {@linkplain PrivilegeCheck
   * privilege check instance}. A privilege is restricted if it is restricted to any role in the
   * given ACL or any of its parents. See note on non-inheritable privileges below.
   *
   * <p>{@linkplain Privilege.NonInheritablePrivilege Non-inheritable} privileges are only effective
   * on the "top" (first) ACL of the given {@linkplain AclChain ACL chain}, but are ignored on any
   * of the parents. For example, a non-inheritable privilege {@code NON_INHERIT} that is
   * <em>granted</em> on the entity's <em>parent</em>, will not be returned as an effective
   * privilege. Similarly, non-inheritable privileges that are <em>restricted</em> on a parent, are
   * not "subtracted" from the set of effective privileges.
   *
   * <p>A privilege is effective if it is granted and not restricted.
   *
   * @param aclChain Represents the chain of ACLs to check. The ACL for the entity must be the first
   *     one in the chain.
   */
  PrivilegeSet effectivePrivilegeSet(@Nonnull AclChain aclChain);
}
