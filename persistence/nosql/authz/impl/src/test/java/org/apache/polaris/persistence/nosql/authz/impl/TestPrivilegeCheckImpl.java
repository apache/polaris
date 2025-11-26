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

import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.polaris.persistence.nosql.authz.api.AclChain;
import org.apache.polaris.persistence.nosql.authz.api.AclEntry;
import org.apache.polaris.persistence.nosql.authz.api.Privilege;
import org.apache.polaris.persistence.nosql.authz.api.PrivilegeCheck;
import org.apache.polaris.persistence.nosql.authz.api.PrivilegeSet;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

@ExtendWith(SoftAssertionsExtension.class)
public class TestPrivilegeCheckImpl {
  @SuppressWarnings("VisibilityModifier")
  @InjectSoftAssertions
  protected SoftAssertions soft;

  private static PrivilegesImpl privileges;

  @BeforeAll
  static void setUp() {
    privileges =
        new PrivilegesImpl(Stream.of(new PrivilegesTestProvider()), new PrivilegesTestRepository());
    JacksonPrivilegesModule.CDIResolver.setResolver(x -> privileges);
  }

  static PrivilegeSet privilegeSet(Privilege... values) {
    return privileges.newPrivilegesSetBuilder().addPrivileges(values).build();
  }

  static AclEntry aclEntry(PrivilegeSet granted, PrivilegeSet restricted) {
    return privileges.newAclEntryBuilder().grant(granted).restrict(restricted).build();
  }

  @Test
  public void restricted() {
    Privilege zero = privileges.byName("zero");
    Privilege two = privileges.byName("two");
    Privilege four = privileges.byName("four");
    Privilege nine = privileges.byName("nine");
    Privilege zeroTwo = privileges.byName("zeroTwo");
    Privilege eightNine = privileges.byName("eightNine");

    AclChain root =
        AclChain.aclChain(
            privileges
                .newAclBuilder()
                .addEntry(
                    "root-g0-r0",
                    privileges.newAclEntryBuilder().grant(zero).restrict(zero).build())
                .addEntry("root-g0", privileges.newAclEntryBuilder().grant(zero).build())
                .addEntry("root-r0", privileges.newAclEntryBuilder().restrict(zero).build())
                .addEntry("root-r4", privileges.newAclEntryBuilder().restrict(four).build())
                .addEntry("x-0", privileges.newAclEntryBuilder().grant(zero).build())
                .build(),
            Optional.empty());
    AclChain parent =
        AclChain.aclChain(
            privileges
                .newAclBuilder()
                .addEntry(
                    "parent-g2-r2",
                    privileges.newAclEntryBuilder().grant(two).restrict(two).grant().build())
                .addEntry("parent-g2", privileges.newAclEntryBuilder().grant(two).grant().build())
                .addEntry("parent-r2", privileges.newAclEntryBuilder().restrict(two).build())
                .addEntry("parent-r0", privileges.newAclEntryBuilder().restrict(zero).build())
                .addEntry("x-0", privileges.newAclEntryBuilder().restrict(zero).build())
                .build(),
            Optional.of(root));
    AclChain leaf =
        AclChain.aclChain(
            privileges
                .newAclBuilder()
                .addEntry(
                    "leaf-g4-r4",
                    privileges.newAclEntryBuilder().grant(four).restrict(four).build())
                .addEntry("leaf-g4", privileges.newAclEntryBuilder().grant(four).build())
                .build(),
            Optional.of(parent));

    PrivilegeCheck privilegeCheck =
        privileges.startPrivilegeCheck(false, Collections.singleton("root-g0"));
    soft.assertThat(privilegeCheck.effectivePrivilegeSet(leaf).contains(zero)).isTrue();
    soft.assertThat(privilegeCheck.effectivePrivilegeSet(leaf).contains(nine)).isFalse();
    soft.assertThat(privilegeCheck.effectivePrivilegeSet(leaf).contains(zeroTwo)).isTrue();
    soft.assertThat(privilegeCheck.effectivePrivilegeSet(leaf).contains(eightNine)).isFalse();

    privilegeCheck = privileges.startPrivilegeCheck(false, Set.of("root-g0", "root-r0"));
    soft.assertThat(privilegeCheck.effectivePrivilegeSet(leaf).contains(zero)).isFalse();
    privilegeCheck = privileges.startPrivilegeCheck(false, Set.of("root-g0-r0"));
    soft.assertThat(privilegeCheck.effectivePrivilegeSet(leaf).contains(zero)).isFalse();

    privilegeCheck = privileges.startPrivilegeCheck(false, Set.of("leaf-g4"));
    soft.assertThat(privilegeCheck.effectivePrivilegeSet(leaf).contains(four)).isTrue();

    privilegeCheck = privileges.startPrivilegeCheck(false, Set.of("leaf-g4", "root-r4"));
    soft.assertThat(privilegeCheck.effectivePrivilegeSet(leaf).contains(four)).isFalse();

    privilegeCheck = privileges.startPrivilegeCheck(false, Set.of("x-0"));
    soft.assertThat(privilegeCheck.effectivePrivilegeSet(leaf).contains(zero)).isFalse();
    soft.assertThat(privilegeCheck.effectivePrivilegeSet(parent).contains(zero)).isFalse();
    soft.assertThat(privilegeCheck.effectivePrivilegeSet(root).contains(zero)).isTrue();
  }

  @ParameterizedTest
  @MethodSource
  public void nonInheritablePrivilegeOnTop(AclChain aclChain, PrivilegeSet expectedEffective) {
    PrivilegeCheck privilegeCheck = privileges.startPrivilegeCheck(false, Set.of("user"));

    PrivilegeSet effective = privilegeCheck.effectivePrivilegeSet(aclChain);
    soft.assertThat(effective).isEqualTo(expectedEffective);
  }

  static Stream<Arguments> nonInheritablePrivilegeOnTop() {
    Privilege one = privileges.byName("one");
    Privilege two = privileges.byName("two");
    Privilege three = privileges.byName("three");
    Privilege nonInherit = privileges.byName("nonInherit");

    return Stream.of(
        arguments(
            // top
            AclChain.aclChain(
                privileges
                    .newAclBuilder()
                    .addEntry("user", aclEntry(privilegeSet(one, nonInherit), privilegeSet()))
                    .build(),
                // mid
                Optional.of(
                    AclChain.aclChain(
                        privileges
                            .newAclBuilder()
                            .addEntry("user", aclEntry(privilegeSet(two), privilegeSet()))
                            .build(),
                        // root
                        Optional.of(
                            AclChain.aclChain(
                                privileges
                                    .newAclBuilder()
                                    .addEntry("user", aclEntry(privilegeSet(three), privilegeSet()))
                                    .build(),
                                Optional.empty()))))),
            privilegeSet(one, two, three, nonInherit)),
        //
        arguments(
            // top
            AclChain.aclChain(
                privileges
                    .newAclBuilder()
                    .addEntry("user", aclEntry(privilegeSet(one), privilegeSet()))
                    .build(),
                // mid
                Optional.of(
                    AclChain.aclChain(
                        privileges
                            .newAclBuilder()
                            .addEntry(
                                "user", aclEntry(privilegeSet(two, nonInherit), privilegeSet()))
                            .build(),
                        // root
                        Optional.of(
                            AclChain.aclChain(
                                privileges
                                    .newAclBuilder()
                                    .addEntry("user", aclEntry(privilegeSet(three), privilegeSet()))
                                    .build(),
                                Optional.empty()))))),
            privilegeSet(one, two, three)),
        //
        arguments(
            // top
            AclChain.aclChain(
                privileges
                    .newAclBuilder()
                    .addEntry("user", aclEntry(privilegeSet(one), privilegeSet()))
                    .build(),
                // mid
                Optional.of(
                    AclChain.aclChain(
                        privileges
                            .newAclBuilder()
                            .addEntry("user", aclEntry(privilegeSet(two), privilegeSet()))
                            .build(),
                        // root
                        Optional.of(
                            AclChain.aclChain(
                                privileges
                                    .newAclBuilder()
                                    .addEntry(
                                        "user",
                                        aclEntry(privilegeSet(three, nonInherit), privilegeSet()))
                                    .build(),
                                Optional.empty()))))),
            privilegeSet(one, two, three))
        //
        );
  }
}
