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
package org.apache.polaris.persistence.nosql.impl.indexes;

import static org.apache.polaris.persistence.nosql.api.index.IndexKey.key;
import static org.apache.polaris.persistence.nosql.api.obj.ObjRef.OBJ_REF_SERIALIZER;
import static org.apache.polaris.persistence.nosql.impl.indexes.IndexesInternal.indexElement;
import static org.apache.polaris.persistence.nosql.impl.indexes.Util.randomObjId;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.util.stream.Stream;
import org.apache.polaris.persistence.nosql.api.obj.ObjRef;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

@ExtendWith(SoftAssertionsExtension.class)
public class TestKeyIndexSets {
  @InjectSoftAssertions SoftAssertions soft;

  @ParameterizedTest
  @MethodSource("keyIndexSetConfigs")
  @Timeout(30) // if this test hits the timeout, then that's a legit bug !!
  void keyIndexSetTests(
      int namespaceLevels, int foldersPerLevel, int tablesPerNamespace, boolean deterministic) {

    var keyIndexTestSet =
        KeyIndexTestSet.<ObjRef>newGenerator()
            .keySet(
                ImmutableRealisticKeySet.builder()
                    .namespaceLevels(namespaceLevels)
                    .foldersPerLevel(foldersPerLevel)
                    .tablesPerNamespace(tablesPerNamespace)
                    .deterministic(deterministic)
                    .build())
            .elementSupplier(key -> indexElement(key, randomObjId()))
            .elementSerializer(OBJ_REF_SERIALIZER)
            .build()
            .generateIndexTestSet();

    soft.assertThatCode(keyIndexTestSet::serialize).doesNotThrowAnyException();
    soft.assertThatCode(keyIndexTestSet::deserialize).doesNotThrowAnyException();
    soft.assertThat(((IndexImpl<ObjRef>) keyIndexTestSet.deserialize()).setModified().serialize())
        .isEqualTo(keyIndexTestSet.serializedSafe());
    soft.assertThatCode(keyIndexTestSet::randomGetKey).doesNotThrowAnyException();
    soft.assertThatCode(
            () -> {
              IndexSpi<ObjRef> deserialized = keyIndexTestSet.deserialize();
              deserialized.add(indexElement(key("zzzzzzzkey"), randomObjId()));
              deserialized.serialize();
            })
        .doesNotThrowAnyException();
    soft.assertThatCode(
            () -> {
              IndexSpi<ObjRef> deserialized = keyIndexTestSet.deserialize();
              for (char c = 'a'; c <= 'z'; c++) {
                deserialized.add(indexElement(key(c + "xkey"), randomObjId()));
              }
              deserialized.serialize();
            })
        .doesNotThrowAnyException();
  }

  static Stream<Arguments> keyIndexSetConfigs() {
    return Stream.of(
        arguments(2, 2, 5, true),
        arguments(2, 2, 5, false),
        arguments(2, 2, 20, true),
        arguments(2, 2, 20, false),
        arguments(5, 5, 50, true),
        arguments(5, 5, 50, false));
  }
}
