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

package org.apache.polaris.persistence.nosql.metastore;

import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.util.List;
import java.util.stream.Stream;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

@ExtendWith(SoftAssertionsExtension.class)
public class TestIdentifier {
  @InjectSoftAssertions SoftAssertions soft;

  @ParameterizedTest
  @MethodSource
  public void storageLocation(String input, List<String> elements) {
    var identifier = Identifier.identifierFromLocationString(input);
    soft.assertThat(identifier.elements()).containsExactlyElementsOf(elements);
    var inKey = identifier.toIndexKey();
    var identifierFromKey = Identifier.indexKeyToIdentifier(inKey);
    soft.assertThat(identifierFromKey).isEqualTo(identifierFromKey);
  }

  static Stream<Arguments> storageLocation() {
    return Stream.of(
        arguments("", List.of()),
        arguments("foo", List.of("foo")),
        arguments("//foo", List.of("foo")),
        arguments("//foo/\\/bar", List.of("foo", "bar")),
        arguments("\\foo/\\/bar", List.of("foo", "bar")),
        arguments("\\/foo/\\/bar", List.of("foo", "bar")),
        arguments("\\/\\foo/\\/bar", List.of("foo", "bar")),
        arguments("foo/\\/bar", List.of("foo", "bar")),
        arguments("foo/\\/bar/", List.of("foo", "bar")),
        arguments("foo/\\/bar\\", List.of("foo", "bar")),
        arguments("foo/\\/bar/\\/", List.of("foo", "bar")));
  }
}
