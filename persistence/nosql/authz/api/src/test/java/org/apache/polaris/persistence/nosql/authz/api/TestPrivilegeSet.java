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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

public class TestPrivilegeSet {
  @Test
  public void testEmptyPrivilegeSetContainsAllFollowsSetContract() {
    var emptyPrivilegeSet = PrivilegeSet.emptyPrivilegeSet();
    var privilege = Privilege.InheritablePrivilege.inheritablePrivilege("read");

    assertThat(emptyPrivilegeSet.containsAll(List.of())).isTrue();
    assertThat(emptyPrivilegeSet.containsAll(List.of(privilege))).isFalse();
  }

  @Test
  public void testEmptyPrivilegeSetToArrayUsesProvidedArray() {
    var emptyPrivilegeSet = PrivilegeSet.emptyPrivilegeSet();
    var privilege = Privilege.InheritablePrivilege.inheritablePrivilege("write");

    var emptyArray = new Privilege[0];
    assertThat(emptyPrivilegeSet.toArray(emptyArray)).isSameAs(emptyArray);

    var arrayWithRoom = new Privilege[] {privilege, privilege};
    assertThat(emptyPrivilegeSet.toArray(arrayWithRoom)).isSameAs(arrayWithRoom);
    assertThat(arrayWithRoom[0]).isNull();
    assertThat(arrayWithRoom[1]).isSameAs(privilege);
  }

  @Test
  public void testEmptyPrivilegeSetEqualityMatchesEmptySetContract() {
    var emptyPrivilegeSet = PrivilegeSet.emptyPrivilegeSet();

    assertThat(emptyPrivilegeSet).isEqualTo(Set.of());
    assertThat(Set.of()).isEqualTo(emptyPrivilegeSet);
    assertThat(emptyPrivilegeSet.hashCode()).isZero();
  }
}
