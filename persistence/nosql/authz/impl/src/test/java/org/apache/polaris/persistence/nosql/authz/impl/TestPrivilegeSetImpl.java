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
import static tools.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES;
import static tools.jackson.databind.MapperFeature.DEFAULT_VIEW_INCLUSION;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import tools.jackson.databind.DatabindException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ArrayNode;
import tools.jackson.dataformat.smile.SmileMapper;

@ExtendWith(SoftAssertionsExtension.class)
public class TestPrivilegeSetImpl {
  @SuppressWarnings("VisibilityModifier")
  @InjectSoftAssertions
  protected SoftAssertions soft;

  private static ObjectMapper jsonMapper;
  private static ObjectMapper smileMapper;
  private static PrivilegesImpl privileges;

  // Needed for tests, don't want to pull in polaris-persistence-nosql-api just for this test
  static final class StorageView {}

  @BeforeAll
  static void setUp() {
    privileges =
        new PrivilegesImpl(Stream.of(new PrivilegesTestProvider()), new PrivilegesTestRepository());
    jsonMapper =
        JsonMapper.builder()
            .addModule(new JacksonPrivilegesModule(() -> privileges))
            .disable(FAIL_ON_UNKNOWN_PROPERTIES)
            .enable(DEFAULT_VIEW_INCLUSION)
            .build();
    smileMapper =
        SmileMapper.builder()
            .findAndAddModules()
            .disable(FAIL_ON_UNKNOWN_PROPERTIES)
            .enable(DEFAULT_VIEW_INCLUSION)
            .build();
    JacksonPrivilegesModule.PrivilegesViaCDI.privilegesForJackson = privileges;
  }

  @SuppressWarnings("RedundantCollectionOperation")
  @ParameterizedTest
  @MethodSource
  public void singlePrivileges(Privilege.IndividualPrivilege privilege, ObjectMapper mapper) {
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
    var serialized = writer.writeValueAsBytes(privilegeSet);
    soft.assertThat(mapper.readValue(serialized, PrivilegeSet.class)).isEqualTo(privilegeSet);

    var jsonNode = mapper.readValue(serialized, JsonNode.class);
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

  @Test
  public void longArrayConversionPreservesByteArrayBitSetRepresentation() {
    for (var bytes :
        new byte[][] {
          {},
          {(byte) 0b1010_0101},
          {(byte) 0x01, (byte) 0x80, (byte) 0x7F},
          {
            (byte) 0x01,
            (byte) 0x23,
            (byte) 0x45,
            (byte) 0x67,
            (byte) 0x89,
            (byte) 0xAB,
            (byte) 0xCD,
            (byte) 0xEF
          },
          {
            (byte) 0xFF,
            (byte) 0x00,
            (byte) 0x10,
            (byte) 0x20,
            (byte) 0x30,
            (byte) 0x40,
            (byte) 0x50,
            (byte) 0x60,
            (byte) 0x70,
            (byte) 0x80
          }
        }) {
      soft.assertThat(BitSet.valueOf(PrivilegeSetImpl.toLongArray(bytes)))
          .isEqualTo(BitSet.valueOf(bytes));
    }
  }

  static Stream<Arguments> singlePrivileges() {
    return bothMappers(
        IntStream.range(0, 128)
            .mapToObj(id -> Privilege.InheritablePrivilege.inheritablePrivilege("foo_" + id))
            .map(Arguments::arguments));
  }

  @ParameterizedTest
  @MethodSource
  public void nameSerialization(PrivilegeSet privilegeSet, ObjectMapper mapper) {
    var serialized = mapper.writeValueAsBytes(privilegeSet);

    var deserialized = mapper.readValue(serialized, PrivilegeSet.class);
    soft.assertThat(deserialized).isEqualTo(privilegeSet);
    soft.assertThat(deserialized).containsExactlyInAnyOrderElementsOf(privilegeSet);

    var jsonNode = mapper.readValue(serialized, JsonNode.class);
    soft.assertThat(jsonNode.isArray()).isTrue();
  }

  static Stream<Arguments> nameSerialization() {
    return bothMappers(
        Stream.concat(
                privileges.all().stream()
                    .map(p -> privileges.newPrivilegesSetBuilder().addPrivilege(p).build()),
                Stream.of(
                    privileges.newPrivilegesSetBuilder().addPrivileges(privileges.all()).build()))
            .map(Arguments::arguments));
  }

  @Test
  public void nameDeserializationRejectsNonStringArrayMembers() {
    soft.assertThatThrownBy(() -> jsonMapper.readValue("[\"zero\",1]", PrivilegeSet.class))
        .isInstanceOf(DatabindException.class)
        .hasMessageContaining("Unexpected JSON token VALUE_NUMBER_INT in privilege array");
  }

  @ParameterizedTest
  @MethodSource
  public void compositeByNameSerialization(
      Privilege composite, Set<Privilege> more, Set<Privilege> inJson, ObjectMapper mapper) {
    var privilegeSet =
        privileges.newPrivilegesSetBuilder().addPrivilege(composite).addPrivileges(more).build();

    var serialized = mapper.writeValueAsBytes(privilegeSet);

    var deserialized = mapper.readValue(serialized, PrivilegeSet.class);
    soft.assertThat(deserialized).isEqualTo(privilegeSet);
    soft.assertThat(deserialized).containsAll(composite.resolved());
    soft.assertThat(deserialized.containsAll(composite.resolved())).isTrue();
    soft.assertThat(deserialized.containsAll(more)).isTrue();

    var jsonNode = mapper.readValue(serialized, JsonNode.class);
    soft.assertThat(jsonNode.isArray()).isTrue();

    var values = new ArrayList<String>();
    var arrayNode = (ArrayNode) jsonNode;
    for (var i = 0; i < arrayNode.size(); i++) {
      values.add(arrayNode.get(i).asString());
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

    return bothMappers(
        Stream.of(
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
                Set.of(oneTwoThree, duplicateOneTwoThree, twoThreeFour))));
  }

  static Stream<Arguments> bothMappers(Stream<Arguments> in) {
    return in.flatMap(
        args -> {
          var argsArray = args.get();
          var jsonArgs = Arrays.copyOf(argsArray, argsArray.length + 1);
          jsonArgs[argsArray.length] = jsonMapper;
          var smileArgs = Arrays.copyOf(argsArray, argsArray.length + 1);
          smileArgs[argsArray.length] = smileMapper;
          return Stream.of(arguments(jsonArgs), arguments(smileArgs));
        });
  }
}
