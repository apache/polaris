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

import static org.apache.polaris.persistence.nosql.api.index.IndexKey.key;
import static org.apache.polaris.persistence.nosql.api.obj.ObjRef.objRef;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.List;
import java.util.OptionalInt;
import java.util.stream.Stream;
import org.apache.polaris.core.persistence.pagination.Page;
import org.apache.polaris.core.persistence.pagination.PageToken;
import org.apache.polaris.persistence.nosql.api.index.IndexKey;
import org.apache.polaris.persistence.nosql.api.obj.ObjRef;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.dataformat.smile.SmileMapper;

@ExtendWith(SoftAssertionsExtension.class)
public class TestNoSqlPaginationToken {
  private static final ObjectMapper JSON_MAPPER = JsonMapper.builder().build();
  private static final ObjectMapper SMILE_MAPPER = SmileMapper.builder().build();

  @InjectSoftAssertions SoftAssertions soft;

  @ParameterizedTest
  @MethodSource("tokens")
  public void serializationRoundtripSmile(ObjRef containerObjRef, IndexKey indexKey) {
    var token = NoSqlPaginationToken.paginationToken(containerObjRef, indexKey);
    var expectedContainerObjRefBytes = containerObjRef.toBytes();
    var expectedKeyBytes = bytes(indexKey.asByteBuffer());

    var serialized = SMILE_MAPPER.writerFor(NoSqlPaginationToken.class).writeValueAsBytes(token);
    var jsonNode = SMILE_MAPPER.readTree(serialized);
    soft.assertThat(jsonNode.asObject().propertyNames()).containsExactlyInAnyOrder("t", "c", "k");
    soft.assertThat(jsonNode.path("t").asString()).isEqualTo(NoSqlPaginationToken.ID);
    soft.assertThat(jsonNode.path("c").asString())
        .isEqualTo(Base64.getEncoder().encodeToString(expectedContainerObjRefBytes));
    soft.assertThat(jsonNode.path("k").asString())
        .isEqualTo(Base64.getEncoder().encodeToString(expectedKeyBytes));
    assertRoundTrip(
        SMILE_MAPPER.readValue(serialized, NoSqlPaginationToken.class),
        containerObjRef,
        indexKey,
        expectedContainerObjRefBytes,
        expectedKeyBytes);
  }

  @ParameterizedTest
  @MethodSource("tokens")
  public void serializationRoundtripJson(ObjRef containerObjRef, IndexKey indexKey) {
    var token = NoSqlPaginationToken.paginationToken(containerObjRef, indexKey);
    var expectedContainerObjRefBytes = containerObjRef.toBytes();
    var expectedKeyBytes = bytes(indexKey.asByteBuffer());

    var serialized = JSON_MAPPER.writerFor(NoSqlPaginationToken.class).writeValueAsString(token);
    var jsonNode = JSON_MAPPER.readTree(serialized);
    soft.assertThat(jsonNode.asObject().propertyNames()).containsExactlyInAnyOrder("t", "c", "k");
    soft.assertThat(jsonNode.path("t").asString()).isEqualTo(NoSqlPaginationToken.ID);
    soft.assertThat(jsonNode.path("c").asString())
        .isEqualTo(Base64.getEncoder().encodeToString(expectedContainerObjRefBytes));
    soft.assertThat(jsonNode.path("k").asString())
        .isEqualTo(Base64.getEncoder().encodeToString(expectedKeyBytes));
    assertRoundTrip(
        JSON_MAPPER.readValue(serialized, NoSqlPaginationToken.class),
        containerObjRef,
        indexKey,
        expectedContainerObjRefBytes,
        expectedKeyBytes);
  }

  @ParameterizedTest
  @MethodSource("tokens")
  public void tokenSurvivesPageTokenRoundTrip(ObjRef containerObjRef, IndexKey indexKey) {
    var token = NoSqlPaginationToken.paginationToken(containerObjRef, indexKey);
    var request = PageToken.fromLimit(10);
    var page = Page.page(request, List.of("item"), token);

    var roundTripped = PageToken.build(page.encodedResponseToken(), null, () -> true);

    soft.assertThat(roundTripped.pageSize()).isEqualTo(OptionalInt.of(10));
    soft.assertThat(roundTripped.valueAs(NoSqlPaginationToken.class))
        .hasValueSatisfying(
            value -> {
              soft.assertThat(value.containerObjRef()).isEqualTo(containerObjRef);
              soft.assertThat(value.key()).isEqualTo(indexKey);
              soft.assertThat(bytes(value.containerObjRefBytes()))
                  .containsExactly(containerObjRef.toBytes());
              soft.assertThat(bytes(value.keyBytes()))
                  .containsExactly(bytes(indexKey.asByteBuffer()));
            });
  }

  private static Stream<Arguments> tokens() {
    return Stream.of(
        arguments(objRef("foo", 123L, 1), key("abc")),
        arguments(objRef("foo", Long.MAX_VALUE, 5), key("A\u0001B")),
        arguments(objRef("bar", Long.MIN_VALUE, 3), key("A\u0001B\u0002C")),
        arguments(objRef("baz", 0L, 1), key("foo\u0000\u0001\u0002\u0000bar")),
        arguments(objRef("qux", 0x1234567890abcdefL, 2), key(Long.MIN_VALUE)),
        arguments(objRef("max", Long.MAX_VALUE, 7), key(Long.MAX_VALUE)));
  }

  private void assertRoundTrip(
      NoSqlPaginationToken actual,
      ObjRef expectedContainerObjRef,
      IndexKey expectedIndexKey,
      byte[] expectedContainerObjRefBytes,
      byte[] expectedKeyBytes) {
    soft.assertThat(actual.containerObjRef()).isEqualTo(expectedContainerObjRef);
    soft.assertThat(actual.key()).isEqualTo(expectedIndexKey);
    soft.assertThat(bytes(actual.containerObjRefBytes()))
        .containsExactly(expectedContainerObjRefBytes);
    soft.assertThat(bytes(actual.keyBytes())).containsExactly(expectedKeyBytes);
  }

  private static byte[] bytes(ByteBuffer buffer) {
    var copy = buffer.duplicate();
    var bytes = new byte[copy.remaining()];
    copy.get(bytes);
    return bytes;
  }
}
