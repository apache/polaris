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
package org.apache.polaris.persistence.nosql.api.obj;

import static org.apache.polaris.persistence.nosql.api.obj.ObjSerializationHelper.contextualReader;
import static org.apache.polaris.persistence.nosql.api.obj.ObjTypes.objTypeById;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Stream;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

@ExtendWith(SoftAssertionsExtension.class)
public class TestGenericObj {
  @InjectSoftAssertions SoftAssertions soft;

  @ParameterizedTest
  @MethodSource
  public void genericObj(ObjType realType, long id, Obj realObj) throws Exception {
    var mapper = new ObjectMapper().findAndRegisterModules();
    // Use some view to exclude the type,id,createdAtMicros,versionToken attributes from being
    // serialized by Jackson.
    var writerAllAttributes = mapper.writer();
    var writer = mapper.writer().withView(Object.class);

    var genericType = objTypeById("genericType_" + UUID.randomUUID());
    var versionToken = realObj.versionToken();

    var json = writer.writeValueAsString(realObj);
    var jsonAllAttributes = writerAllAttributes.writeValueAsString(realObj);
    var genericObj =
        contextualReader(mapper, genericType, id, 0, versionToken, realObj.createdAtMicros())
            .readValue(json, genericType.targetClass());
    var genericObjAllAttributes =
        contextualReader(mapper, genericType, id, 0, versionToken, realObj.createdAtMicros())
            .readValue(jsonAllAttributes, genericType.targetClass());
    soft.assertThat(genericObj)
        .isEqualTo(genericObjAllAttributes)
        .isInstanceOf(GenericObj.class)
        .extracting(GenericObj.class::cast)
        .extracting(GenericObj::id, GenericObj::type)
        .containsExactly(realObj.id(), genericType);

    var jsonGeneric = writer.writeValueAsString(genericObj);
    var jsonGenericAllAttributes = writerAllAttributes.writeValueAsString(genericObj);
    var deserRealObj =
        contextualReader(mapper, realType, id, 1, versionToken, realObj.createdAtMicros())
            .readValue(jsonGeneric, realType.targetClass());
    var deserRealObjAllAttributes =
        contextualReader(mapper, realType, id, 1, versionToken, realObj.createdAtMicros())
            .readValue(jsonGenericAllAttributes, realType.targetClass());
    soft.assertThat(deserRealObj).isEqualTo(realObj).isEqualTo(deserRealObjAllAttributes);
  }

  static Stream<Arguments> genericObj() {
    // We don't persist anything, so we can reuse this ID.
    var id = ThreadLocalRandom.current().nextLong();

    return Stream.of(
        arguments(
            SimpleTestObj.TYPE,
            id,
            SimpleTestObj.builder()
                .id(id)
                .createdAtMicros(123L)
                .addList("one", "two", "three")
                .putMap("a", "A")
                .putMap("b", "B")
                .text("some text")
                .build()),
        //
        arguments(
            VersionedTestObj.TYPE,
            id,
            VersionedTestObj.builder()
                .id(id)
                .createdAtMicros(123L)
                .someValue("some value")
                .versionToken("my version token")
                .build()));
  }
}
