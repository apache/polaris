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
import java.util.Set;
import org.apache.polaris.persistence.nosql.authz.api.AclChain;
import org.apache.polaris.persistence.nosql.authz.api.PrivilegeCheck;
import org.apache.polaris.persistence.nosql.authz.api.PrivilegeSet;
import org.apache.polaris.persistence.nosql.authz.api.Privileges;

record PrivilegeCheckImpl(Set<String> roleIds, Privileges privileges) implements PrivilegeCheck {

  @Override
  public PrivilegeSet effectivePrivilegeSet(@Nonnull AclChain aclChain) {
    // Collect granted+restricted from the direct ACL
    PrivilegeSet.PrivilegeSetBuilder topGranted = privileges.newPrivilegesSetBuilder();
    PrivilegeSet.PrivilegeSetBuilder topRestricted = privileges.newPrivilegesSetBuilder();
    aclChain
        .acl()
        .entriesForRoleIds(
            roleIds,
            aclEntry -> {
              topGranted.addPrivileges(aclEntry.granted());
              topRestricted.addPrivileges(aclEntry.restricted());
            });

    // Collect granted+restricted from the parent ACLs
    PrivilegeSet.PrivilegeSetBuilder granted = privileges.newPrivilegesSetBuilder();
    PrivilegeSet.PrivilegeSetBuilder restricted = privileges.newPrivilegesSetBuilder();
    while (aclChain.parent().isPresent()) {
      aclChain = aclChain.parent().get();
      aclChain
          .acl()
          .entriesForRoleIds(
              roleIds,
              aclEntry -> {
                granted.addPrivileges(aclEntry.granted());
                restricted.addPrivileges(aclEntry.restricted());
              });
    }

    // Remove non-inheritable privileges from the ACLs of the parents. Since those are not
    // inheritable, they do not apply.
    PrivilegeSet nonInheritable = privileges.nonInheritablePrivileges();
    granted.removePrivileges(nonInheritable);
    restricted.removePrivileges(nonInheritable);

    // Add all privileges from the "direct" ACL, this includes the non-inheritable privileges
    granted.addPrivileges(topGranted.build());
    restricted.addPrivileges(topRestricted.build());

    // Remove restricted privileges from the granted privileges, `granted` now contains the
    // effective privileges
    granted.removePrivileges(restricted.build());

    return granted.build();
  }
}
