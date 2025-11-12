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
package org.apache.polaris.persistence.nosql.authz.impl;

import jakarta.annotation.Nonnull;
import java.util.Collection;
import org.apache.polaris.persistence.nosql.authz.api.AclEntry;
import org.apache.polaris.persistence.nosql.authz.api.ImmutableAclEntry;
import org.apache.polaris.persistence.nosql.authz.api.Privilege;
import org.apache.polaris.persistence.nosql.authz.api.PrivilegeSet;
import org.apache.polaris.persistence.nosql.authz.api.Privileges;

final class AclEntryBuilderImpl implements AclEntry.AclEntryBuilder {
  private final PrivilegeSet.PrivilegeSetBuilder granted;
  private final PrivilegeSet.PrivilegeSetBuilder restricted;

  AclEntryBuilderImpl(Privileges privileges) {
    this.granted = privileges.newPrivilegesSetBuilder();
    this.restricted = privileges.newPrivilegesSetBuilder();
  }

  AclEntryBuilderImpl(Privileges privileges, AclEntry aclEntry) {
    this.granted = privileges.newPrivilegesSetBuilder().addPrivileges(aclEntry.granted());
    this.restricted = privileges.newPrivilegesSetBuilder().addPrivileges(aclEntry.restricted());
  }

  @Override
  public AclEntry.AclEntryBuilder grant(@Nonnull Privilege privilege) {
    this.granted.addPrivilege(privilege);
    return this;
  }

  @Override
  public AclEntry.AclEntryBuilder grant(@Nonnull Privilege... privileges) {
    this.granted.addPrivileges(privileges);
    return this;
  }

  @Override
  public AclEntry.AclEntryBuilder grant(@Nonnull Collection<? extends Privilege> privileges) {
    this.granted.addPrivileges(privileges);
    return this;
  }

  @Override
  public AclEntry.AclEntryBuilder grant(@Nonnull PrivilegeSet privileges) {
    this.granted.addPrivileges(privileges);
    return this;
  }

  @Override
  public AclEntry.AclEntryBuilder revoke(@Nonnull Privilege privilege) {
    this.granted.removePrivilege(privilege);
    return this;
  }

  @Override
  public AclEntry.AclEntryBuilder revoke(@Nonnull Privilege... privileges) {
    this.granted.removePrivileges(privileges);
    return this;
  }

  @Override
  public AclEntry.AclEntryBuilder revoke(@Nonnull Collection<? extends Privilege> privileges) {
    this.granted.removePrivileges(privileges);
    return this;
  }

  @Override
  public AclEntry.AclEntryBuilder revoke(@Nonnull PrivilegeSet privileges) {
    this.granted.removePrivileges(privileges);
    return this;
  }

  @Override
  public AclEntry.AclEntryBuilder restrict(@Nonnull Privilege privilege) {
    this.restricted.addPrivilege(privilege);
    return this;
  }

  @Override
  public AclEntry.AclEntryBuilder restrict(@Nonnull Privilege... privileges) {
    this.restricted.addPrivileges(privileges);
    return this;
  }

  @Override
  public AclEntry.AclEntryBuilder restrict(@Nonnull Collection<? extends Privilege> privileges) {
    this.restricted.addPrivileges(privileges);
    return this;
  }

  @Override
  public AclEntry.AclEntryBuilder restrict(@Nonnull PrivilegeSet privileges) {
    this.restricted.addPrivileges(privileges);
    return this;
  }

  @Override
  public AclEntry.AclEntryBuilder unrestrict(@Nonnull Privilege privilege) {
    this.restricted.removePrivilege(privilege);
    return this;
  }

  @Override
  public AclEntry.AclEntryBuilder unrestrict(@Nonnull Privilege... privileges) {
    this.restricted.removePrivileges(privileges);
    return this;
  }

  @Override
  public AclEntry.AclEntryBuilder unrestrict(@Nonnull Collection<? extends Privilege> privileges) {
    this.restricted.removePrivileges(privileges);
    return this;
  }

  @Override
  public AclEntry.AclEntryBuilder unrestrict(@Nonnull PrivilegeSet privileges) {
    this.restricted.removePrivileges(privileges);
    return this;
  }

  @Override
  public AclEntry build() {
    return ImmutableAclEntry.of(this.granted.build(), this.restricted.build());
  }
}
