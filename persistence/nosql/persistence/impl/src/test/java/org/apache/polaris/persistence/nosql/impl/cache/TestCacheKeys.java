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
package org.apache.polaris.persistence.nosql.impl.cache;

import static org.apache.polaris.persistence.nosql.api.Realms.SYSTEM_REALM_ID;
import static org.apache.polaris.persistence.nosql.api.obj.ObjRef.objRef;
import static org.apache.polaris.persistence.nosql.api.obj.ObjTypes.objTypeById;
import static org.apache.polaris.persistence.nosql.impl.cache.CaffeineCacheBackend.cacheKeyObjId;
import static org.apache.polaris.persistence.nosql.impl.cache.CaffeineCacheBackend.cacheKeyValueNegative;
import static org.apache.polaris.persistence.nosql.impl.cache.CaffeineCacheBackend.cacheKeyValueObj;
import static org.apache.polaris.persistence.nosql.impl.cache.CaffeineCacheBackend.cacheKeyValueObjRead;
import static org.apache.polaris.persistence.nosql.impl.cache.CaffeineCacheBackend.cacheKeyValueReference;
import static org.apache.polaris.persistence.nosql.impl.cache.CaffeineCacheBackend.cacheKeyValueReferenceRead;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.util.stream.Stream;
import org.apache.polaris.persistence.nosql.api.obj.ImmutableGenericObj;
import org.apache.polaris.persistence.nosql.api.ref.Reference;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

@ExtendWith(SoftAssertionsExtension.class)
public class TestCacheKeys {
  @InjectSoftAssertions protected SoftAssertions soft;

  @ParameterizedTest
  @MethodSource
  public void referenceKeys(String realmId, String referenceName) {
    var read1 = cacheKeyValueReferenceRead(realmId, referenceName);
    var read2 = cacheKeyValueReferenceRead(realmId, referenceName);
    soft.assertThat(read2).isEqualTo(read1);
    soft.assertThat(read2.hashCode()).isEqualTo(read1.hashCode());
    soft.assertThat(read1).isEqualTo(read2);

    var readDiffRealm = cacheKeyValueReferenceRead(SYSTEM_REALM_ID, referenceName);
    soft.assertThat(readDiffRealm).isNotEqualTo(read1);

    var readDiffName = cacheKeyValueReferenceRead(realmId, referenceName + 'a');
    soft.assertThat(readDiffName).isNotEqualTo(read1);

    var write1 =
        cacheKeyValueReference(
            realmId,
            Reference.builder().name(referenceName).previousPointers().createdAtMicros(42L).build(),
            0L);
    soft.assertThat(write1).isEqualTo(read1);
    soft.assertThat(read1).isEqualTo(write1);
    var write2 =
        cacheKeyValueReference(
            realmId,
            Reference.builder().name(referenceName).previousPointers().createdAtMicros(42L).build(),
            2L);
    soft.assertThat(write2).isEqualTo(read1);
    soft.assertThat(read1).isEqualTo(write2);
    soft.assertThat(write2).isEqualTo(write1);
    soft.assertThat(write1).isEqualTo(write2);
  }

  static Stream<Arguments> referenceKeys() {
    return Stream.of(
        arguments("realm", ""), arguments("realm", "ref"), arguments("", "ref"), arguments("", ""));
  }

  @ParameterizedTest
  @MethodSource
  public void objKeys(String realmId, String type, long id) {
    var read1 = cacheKeyValueObjRead(realmId, objRef(type, id));
    var read2 = cacheKeyValueObjRead(realmId, objRef(type, id, 1));
    soft.assertThat(read2).isEqualTo(read1);
    soft.assertThat(read2.hashCode()).isEqualTo(read1.hashCode());
    soft.assertThat(read1).isEqualTo(read2);

    var negative1 = cacheKeyValueNegative(realmId, cacheKeyObjId(objRef(type, id)), 0L);
    soft.assertThat(negative1).isEqualTo(read1);
    soft.assertThat(read1).isEqualTo(negative1);
    var negative2 = cacheKeyValueNegative(realmId, cacheKeyObjId(objRef(type, id, 1)), 123L);
    soft.assertThat(negative2).isEqualTo(read1);
    soft.assertThat(read1).isEqualTo(negative2);

    var obj1 =
        cacheKeyValueObj(
            realmId,
            ImmutableGenericObj.builder()
                .type(objTypeById(type))
                .id(id)
                .numParts(42)
                .createdAtMicros(123)
                .build(),
            0L);
    var obj2 =
        cacheKeyValueObj(
            realmId,
            ImmutableGenericObj.builder()
                .type(objTypeById(type))
                .id(id)
                .numParts(1)
                .createdAtMicros(123)
                .build(),
            0L);
    var obj3 =
        cacheKeyValueObj(
            realmId,
            ImmutableGenericObj.builder()
                .type(objTypeById(type))
                .id(id)
                .numParts(1)
                .createdAtMicros(42)
                .build(),
            0L);
    soft.assertThat(obj2).isEqualTo(obj1);
    soft.assertThat(obj1).isEqualTo(obj2);
    soft.assertThat(obj3).isEqualTo(obj1);
    soft.assertThat(obj1).isEqualTo(obj3);
    soft.assertThat(obj1).isEqualTo(read1);
    soft.assertThat(read1).isEqualTo(obj1);
    soft.assertThat(obj1).isEqualTo(negative1);
    soft.assertThat(negative1).isEqualTo(obj1);
  }

  static Stream<Arguments> objKeys() {
    return Stream.of(arguments("realm", "type1", 42L), arguments("", "x", 43L));
  }
}
