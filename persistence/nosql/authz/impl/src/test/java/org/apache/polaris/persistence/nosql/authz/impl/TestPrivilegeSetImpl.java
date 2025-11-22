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

import static java.util.Collections.singleton;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import java.util.ArrayList;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.polaris.persistence.nosql.authz.api.Privilege;
import org.apache.polaris.persistence.nosql.authz.api.PrivilegeSet;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

@ExtendWith(SoftAssertionsExtension.class)
public class TestPrivilegeSetImpl {
  @SuppressWarnings("VisibilityModifier")
  @InjectSoftAssertions
  protected SoftAssertions soft;

  private static ObjectMapper mapper;
  private static PrivilegesImpl privileges;

  // Needed for tests, don't want to pull in polaris-persistence-nosql-api just for this test
  static final class StorageView {}

  @BeforeAll
  static void setUp() {
    mapper = new ObjectMapper().findAndRegisterModules();
    privileges =
        new PrivilegesImpl(Stream.of(new PrivilegesTestProvider()), new PrivilegesTestRepository());
    JacksonPrivilegesModule.CDIResolver.setResolver(x -> privileges);
  }

  @SuppressWarnings("RedundantCollectionOperation")
  @ParameterizedTest
  @MethodSource
  public void singlePrivileges(Privilege.IndividualPrivilege privilege) throws Exception {
    var privilegeSet = privileges.newPrivilegesSetBuilder().addPrivilege(privilege).build();
    soft.assertThat(privilegeSet.isEmpty()).isFalse();
    soft.assertThat(privilegeSet.contains(privilege)).isTrue();
    soft.assertThat(privilegeSet.containsAll(singleton(privilege))).isTrue();
    soft.assertThat(
            privileges
                .newPrivilegesSetBuilder()
                .addPrivileges(privilegeSet)
                .build()
                .contains(privilege))
        .isTrue();
    soft.assertThat(
            privileges
                .newPrivilegesSetBuilder()
                .addPrivileges(privilegeSet)
                .removePrivilege(privilege)
                .build()
                .contains(privilege))
        .isFalse();

    var index = privileges.idForPrivilege(privilege) >> 3;
    soft.assertThat(privilegeSet.toByteArray()).hasSize(index + 1);
    soft.assertThat(
            privileges
                .newPrivilegesSetBuilder()
                .addPrivileges(privilegeSet)
                .removePrivilege(privilege)
                .build()
                .toByteArray())
        .hasSize(0);
    soft.assertThat(
            privileges
                .newPrivilegesSetBuilder()
                .addPrivileges(privilegeSet)
                .removePrivilege(privilege)
                .build()
                .isEmpty())
        .isTrue();

    var writer = mapper.writerWithView(StorageView.class);
    var json = writer.writeValueAsString(privilegeSet);
    soft.assertThat(mapper.readValue(json, PrivilegeSet.class)).isEqualTo(privilegeSet);

    var jsonNode = mapper.readValue(json, JsonNode.class);
    soft.assertThat(jsonNode.isArray()).isFalse();

    for (var j = 0; j < 256; j++) {
      if (j == privileges.idForPrivilege(privilege)) {
        continue;
      }
      var other = Privilege.InheritablePrivilege.inheritablePrivilege("zero");
      soft.assertThat(privilegeSet.contains(other)).isFalse();
      soft.assertThat(privilegeSet.containsAll(singleton(other))).isFalse();
      soft.assertThat(
              privileges
                  .newPrivilegesSetBuilder()
                  .addPrivileges(privilegeSet)
                  .build()
                  .contains(other))
          .isFalse();
      soft.assertThat(
              privileges
                  .newPrivilegesSetBuilder()
                  .addPrivileges(privilegeSet)
                  .addPrivilege(other)
                  .build()
                  .contains(other))
          .isTrue();
    }
  }

  static Stream<Privilege.IndividualPrivilege> singlePrivileges() {
    return IntStream.range(0, 128)
        .mapToObj(id -> Privilege.InheritablePrivilege.inheritablePrivilege("foo_" + id));
  }

  @ParameterizedTest
  @MethodSource
  public void nameSerialization(PrivilegeSet privilegeSet) throws Exception {
    var json = mapper.writeValueAsString(privilegeSet);

    var deserialized = mapper.readValue(json, PrivilegeSet.class);
    soft.assertThat(deserialized).isEqualTo(privilegeSet);
    soft.assertThat(deserialized).containsExactlyInAnyOrderElementsOf(privilegeSet);

    var jsonNode = mapper.readValue(json, JsonNode.class);
    soft.assertThat(jsonNode.isArray()).isTrue();
  }

  static Stream<PrivilegeSet> nameSerialization() {
    return Stream.concat(
        privileges.all().stream()
            .map(p -> privileges.newPrivilegesSetBuilder().addPrivilege(p).build()),
        Stream.of(privileges.newPrivilegesSetBuilder().addPrivileges(privileges.all()).build()));
  }

  @ParameterizedTest
  @MethodSource
  public void compositeByNameSerialization(
      Privilege composite, Set<Privilege> more, Set<Privilege> inJson) throws Exception {
    var privilegeSet =
        privileges.newPrivilegesSetBuilder().addPrivilege(composite).addPrivileges(more).build();

    var json = mapper.writeValueAsString(privilegeSet);

    var deserialized = mapper.readValue(json, PrivilegeSet.class);
    soft.assertThat(deserialized).isEqualTo(privilegeSet);
    soft.assertThat(deserialized).containsAll(composite.resolved());
    soft.assertThat(deserialized.containsAll(composite.resolved())).isTrue();
    soft.assertThat(deserialized.containsAll(more)).isTrue();

    var jsonNode = mapper.readValue(json, JsonNode.class);
    soft.assertThat(jsonNode.isArray()).isTrue();

    var values = new ArrayList<String>();
    var arrayNode = (ArrayNode) jsonNode;
    for (var i = 0; i < arrayNode.size(); i++) {
      values.add(arrayNode.get(i).asText());
    }

    soft.assertThat(values)
        .containsExactlyInAnyOrderElementsOf(
            inJson.stream().map(Privilege::name).collect(Collectors.toList()));
  }

  static Stream<Arguments> compositeByNameSerialization() {
    var oneTwoThree = privileges.byName("oneTwoThree");
    var duplicateOneTwoThree = privileges.byName("duplicateOneTwoThree");
    var twoThreeFour = privileges.byName("twoThreeFour");
    var fiveSix = privileges.byName("fiveSix");
    var three = privileges.byName("three");
    var five = privileges.byName("five");
    var seven = privileges.byName("seven");

    return Stream.of(
        arguments(oneTwoThree, Set.of(), Set.of(oneTwoThree, duplicateOneTwoThree)),
        arguments(
            oneTwoThree,
            Set.of(three, five, seven),
            Set.of(oneTwoThree, duplicateOneTwoThree, five, seven)),
        arguments(duplicateOneTwoThree, Set.of(), Set.of(oneTwoThree, duplicateOneTwoThree)),
        arguments(twoThreeFour, Set.of(), Set.of(twoThreeFour)),
        arguments(twoThreeFour, Set.of(three, five, seven), Set.of(twoThreeFour, five, seven)),
        arguments(fiveSix, Set.of(), Set.of(fiveSix)),
        arguments(fiveSix, Set.of(three, five, seven), Set.of(fiveSix, three, seven)),
        arguments(
            twoThreeFour,
            Set.of(oneTwoThree),
            Set.of(oneTwoThree, duplicateOneTwoThree, twoThreeFour)));
  }
}
