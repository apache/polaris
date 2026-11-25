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
import static org.apache.polaris.persistence.nosql.api.obj.ObjSerializationHelper.readersFor;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static tools.jackson.databind.MapperFeature.DEFAULT_VIEW_INCLUSION;

import com.fasterxml.jackson.annotation.JacksonInject;
import java.util.Map;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import tools.jackson.databind.InjectableValues;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.ObjectReader;
import tools.jackson.databind.exc.MissingInjectableValueException;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.dataformat.smile.SmileMapper;

class TestObjSerializationHelper {
  @ParameterizedTest
  @MethodSource("mappers")
  void unknownInjectionKeyFailsLikeStandardProvider(ObjectMapper mapper) {
    var payload = mapper.writeValueAsBytes(Map.of());
    var reader = readersFor(mapper).get(UnknownInjection.class);
    var contextual = contextualReader(reader, SimpleTestObj.TYPE, 1L, 0, null, 0L);
    var standard = reader.with(new InjectableValues.Std());

    for (var candidate : new ObjectReader[] {contextual, standard}) {
      assertThatThrownBy(() -> candidate.readValue(payload))
          .isInstanceOf(MissingInjectableValueException.class)
          .hasMessageContaining("No injectable value with id 'unknown' found");
    }
  }

  static Stream<ObjectMapper> mappers() {
    return Stream.of(JsonMapper.builder().build(), SmileMapper.builder().build());
  }

  private record UnknownInjection(@JacksonInject("unknown") String value) {}

  @Test
  void concurrentReadsKeepMetadataAndMapperConfigurationIsolated() {
    ObjectMapper[] mappers = {
      JsonMapper.builder().findAndAddModules().enable(DEFAULT_VIEW_INCLUSION).build(),
      SmileMapper.builder().findAndAddModules().enable(DEFAULT_VIEW_INCLUSION).build()
    };
    for (var mapper : mappers) {
      var readers = readersFor(mapper);
      var payload =
          mapper
              .writer()
              .withView(Object.class)
              .writeValueAsBytes(SimpleTestObj.builder().id(0).text("payload").build());
      IntStream.range(1, 129)
          .parallel()
          .forEach(id -> {
            var token = id % 2 == 0 ? null : "version-" + id;
            SimpleTestObj obj =
                contextualReader(
                        readers.get(SimpleTestObj.class),
                        SimpleTestObj.TYPE,
                        id,
                        id + 1,
                        token,
                        id + 1000L)
                    .readValue(payload);
            assertThat(obj.id()).isEqualTo(id);
            assertThat(obj.numParts()).isEqualTo(id + 1);
            assertThat(obj.createdAtMicros()).isEqualTo(id + 1000L);
            assertThat(obj.versionToken()).isEqualTo(token);
            assertThat(obj.text()).isEqualTo("payload");
          });
    }
  }
}
