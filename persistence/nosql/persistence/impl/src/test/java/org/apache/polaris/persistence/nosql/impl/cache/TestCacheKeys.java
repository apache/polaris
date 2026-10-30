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
import static org.apache.polaris.persistence.nosql.impl.cache.CaffeineCacheBackend.cacheKeyObjRead;
import static org.apache.polaris.persistence.nosql.impl.cache.CaffeineCacheBackend.cacheKeyReferenceRead;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.util.stream.Stream;
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
    var read1 = cacheKeyReferenceRead(realmId, referenceName);
    var read2 = cacheKeyReferenceRead(realmId, referenceName);
    soft.assertThat(read2).isEqualTo(read1);
    soft.assertThat(read2.hashCode()).isEqualTo(read1.hashCode());
    soft.assertThat(read1).isEqualTo(read2);

    var readDiffRealm = cacheKeyReferenceRead(SYSTEM_REALM_ID, referenceName);
    soft.assertThat(readDiffRealm).isNotEqualTo(read1);

    var readDiffName = cacheKeyReferenceRead(realmId, referenceName + 'a');
    soft.assertThat(readDiffName).isNotEqualTo(read1);
  }

  static Stream<Arguments> referenceKeys() {
    return Stream.of(
        arguments("realm", ""), arguments("realm", "ref"), arguments("", "ref"), arguments("", ""));
  }

  @ParameterizedTest
  @MethodSource
  public void objKeys(String realmId, String type, long id) {
    var read1 = cacheKeyObjRead(realmId, objRef(type, id));
    var read2 = cacheKeyObjRead(realmId, objRef(type, id, 1));
    soft.assertThat(read2).isEqualTo(read1);
    soft.assertThat(read2.hashCode()).isEqualTo(read1.hashCode());
    soft.assertThat(read1).isEqualTo(read2);

    var multipart = cacheKeyObjRead(realmId, objRef(type, id, 42));
    soft.assertThat(multipart).isEqualTo(read1);
    soft.assertThat(multipart.hashCode()).isEqualTo(read1.hashCode());
    soft.assertThat(cacheKeyObjRead(SYSTEM_REALM_ID, objRef(type, id))).isNotEqualTo(read1);
    soft.assertThat(cacheKeyObjRead(realmId, objRef(type, id + 1))).isNotEqualTo(read1);
    soft.assertThat(cacheKeyObjRead(realmId, objRef(type + "x", id))).isNotEqualTo(read1);
    soft.assertThat(cacheKeyReferenceRead(realmId, type)).isNotEqualTo(read1);
  }

  static Stream<Arguments> objKeys() {
    return Stream.of(arguments("realm", "type1", 42L), arguments("", "x", 43L));
  }
}
