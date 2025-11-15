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

import static org.apache.polaris.persistence.nosql.authz.api.Privilege.AlternativePrivilege.alternativePrivilege;
import static org.apache.polaris.persistence.nosql.authz.api.Privilege.CompositePrivilege.compositePrivilege;
import static org.apache.polaris.persistence.nosql.authz.api.Privilege.InheritablePrivilege.inheritablePrivilege;

import java.util.Set;
import java.util.stream.Stream;
import org.apache.polaris.persistence.nosql.authz.api.Privilege;
import org.apache.polaris.persistence.nosql.authz.api.Privileges;
import org.apache.polaris.persistence.nosql.authz.spi.PrivilegeDefinition;
import org.apache.polaris.persistence.nosql.authz.spi.PrivilegesProvider;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.assertj.core.util.Lists;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(SoftAssertionsExtension.class)
public class TestPrivilegesImpl {
  @SuppressWarnings("VisibilityModifier")
  @InjectSoftAssertions
  protected SoftAssertions soft;

  @Test
  public void duplicateNames() {
    soft.assertThatIllegalStateException()
        .isThrownBy(
            () ->
                testPrivileges(
                    testPrivilegesProvider(
                        "PROVIDER1", inheritablePrivilege("foo"), inheritablePrivilege("foo"))))
        .withMessage("Duplicate privilege definition for name 'foo'");

    soft.assertThatIllegalStateException()
        .isThrownBy(
            () ->
                testPrivileges(
                    testPrivilegesProvider("PROVIDER1", inheritablePrivilege("foo")),
                    testPrivilegesProvider("PROVIDER2", inheritablePrivilege("foo"))))
        .withMessage("Duplicate privilege definition for name 'foo'");
  }

  static PrivilegesProvider testPrivilegesProvider(String name, Privilege... privileges) {
    return new PrivilegesProvider() {
      @Override
      public Stream<PrivilegeDefinition> privilegeDefinitions() {
        return Stream.of(privileges).map(p -> PrivilegeDefinition.builder().privilege(p).build());
      }

      @Override
      public String name() {
        return name;
      }
    };
  }

  static Privileges testPrivileges(PrivilegesProvider... privilegeProviders) {
    var privilegesRepository = new PrivilegesTestRepository();
    return new PrivilegesImpl(Stream.of(privilegeProviders), privilegesRepository);
  }

  @Test
  public void compositeAndAlternativePrivileges() {
    var foo = inheritablePrivilege("foo");
    var bar = inheritablePrivilege("bar");
    var baz = inheritablePrivilege("baz");
    var fooBarBaz = compositePrivilege("foo-bar-baz", foo, bar, baz);
    var alt1 = alternativePrivilege("alt1", foo, bar);

    var meow = inheritablePrivilege("meow");
    var woof = inheritablePrivilege("woof");
    var meowWoof = compositePrivilege("meow-woof", meow, woof);
    var alt2 = alternativePrivilege("alt2", meow, woof);

    var privileges =
        testPrivileges(
            testPrivilegesProvider("PROVIDER", foo, bar, baz, fooBarBaz, meow, woof, meowWoof));

    var privilegeSet = privileges.newPrivilegesSetBuilder().addPrivilege(fooBarBaz).build();
    soft.assertThat(privileges.newPrivilegesSetBuilder().addPrivileges(foo, bar, baz).build())
        .isEqualTo(privilegeSet);
    soft.assertThat(privilegeSet)
        .isEqualTo(privileges.newPrivilegesSetBuilder().addPrivileges(foo, bar, baz).build());
    soft.assertThat(privilegeSet.contains(fooBarBaz)).isTrue();
    soft.assertThat(privilegeSet.contains(alt1)).isTrue();
    soft.assertThat(privilegeSet.contains(foo)).isTrue();
    soft.assertThat(privilegeSet.contains(bar)).isTrue();
    soft.assertThat(privilegeSet.contains(baz)).isTrue();
    soft.assertThat(privilegeSet.contains(meowWoof)).isFalse();
    soft.assertThat(privilegeSet.contains(meow)).isFalse();
    soft.assertThat(privilegeSet.contains(woof)).isFalse();

    var privilegeSetList = Lists.newArrayList(privilegeSet.iterator(privileges));
    soft.assertThat(privilegeSetList).containsExactlyInAnyOrder(foo, bar, baz);
    soft.assertThat(privilegeSetList).doesNotContainAnyElementsOf(Set.of(woof, meow));
    soft.assertThat(privilegeSetList).doesNotContainAnyElementsOf(Set.of(meowWoof));

    privilegeSet = privileges.newPrivilegesSetBuilder().addPrivileges(foo, baz).build();
    soft.assertThat(privilegeSet.contains(alt1)).isTrue();

    privilegeSetList = Lists.newArrayList(privilegeSet.iterator(privileges));
    soft.assertThat(privilegeSetList).containsExactlyInAnyOrder(foo, baz);

    privilegeSet = privileges.newPrivilegesSetBuilder().addPrivileges(meowWoof).build();
    soft.assertThat(privileges.newPrivilegesSetBuilder().addPrivileges(meow, woof).build())
        .isEqualTo(privilegeSet);
    soft.assertThat(privilegeSet)
        .isEqualTo(privileges.newPrivilegesSetBuilder().addPrivileges(meowWoof).build());
    soft.assertThat(privilegeSet.contains(fooBarBaz)).isFalse();
    soft.assertThat(privilegeSet.contains(alt1)).isFalse();
    soft.assertThat(privilegeSet.contains(foo)).isFalse();
    soft.assertThat(privilegeSet.contains(bar)).isFalse();
    soft.assertThat(privilegeSet.contains(baz)).isFalse();
    soft.assertThat(privilegeSet.contains(meowWoof)).isTrue();
    soft.assertThat(privilegeSet.contains(meow)).isTrue();
    soft.assertThat(privilegeSet.contains(woof)).isTrue();
    soft.assertThat(privilegeSet.contains(alt2)).isTrue();

    privilegeSetList = Lists.newArrayList(privilegeSet.iterator(privileges));
    soft.assertThat(privilegeSetList).containsExactlyInAnyOrder(woof, meow);
    soft.assertThat(privilegeSetList).doesNotContainAnyElementsOf(Set.of(foo, bar, baz));
  }
}
