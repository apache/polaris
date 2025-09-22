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

package org.apache.polaris.persistence.nosql.api;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.stream.IntStream;
import org.apache.polaris.persistence.nosql.api.obj.ObjRef;
import org.apache.polaris.persistence.nosql.api.obj.SimpleTestObj;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junitpioneer.jupiter.cartesian.CartesianTest;
import org.junitpioneer.jupiter.cartesian.CartesianTest.Values;

@ExtendWith(SoftAssertionsExtension.class)
public class TestPersistence {
  @InjectSoftAssertions SoftAssertions soft;

  @CartesianTest
  public void bucketizedBulkFetches(
      @Values(ints = {1, 3, 5, 13}) int fetchSize,
      @Values(ints = {1, 3, 5, 15, 7, 13, 30}) int totalSize) {
    var params =
        PersistenceParams.BuildablePersistenceParams.builder()
            .bucketizedBulkFetchSize(fetchSize)
            .build();

    var persistence = mock(Persistence.class);
    when(persistence.params()).thenReturn(params);

    when(persistence.bucketizedBulkFetches(any(), any())).thenCallRealMethod();

    var objRefIntFunction = (IntFunction<ObjRef>) i -> ObjRef.objRef(SimpleTestObj.TYPE, i);
    var objRefs = IntStream.range(0, totalSize).mapToObj(objRefIntFunction).toList();
    var toObj =
        (Function<ObjRef, SimpleTestObj>)
            objRef ->
                objRef.id() < totalSize ? SimpleTestObj.builder().id(objRef.id()).build() : null;

    var fetchManyInvocations = (totalSize + fetchSize - 1) / fetchSize;
    var whenFetchMany = when(persistence.fetchMany(any(), any(ObjRef[].class)));
    for (int i = 0; i < fetchManyInvocations; i++) {
      // Construct a full chunk of size 'fetchSize'
      var answer =
          IntStream.range(i * fetchSize, (i + 1) * fetchSize)
              .mapToObj(objRefIntFunction)
              .map(toObj)
              .toArray(SimpleTestObj[]::new);

      whenFetchMany = whenFetchMany.thenReturn(answer);
    }

    var result = persistence.bucketizedBulkFetches(objRefs.stream(), SimpleTestObj.class).toList();

    soft.assertThat(result)
        .hasSize(totalSize)
        .map(SimpleTestObj::id)
        .containsExactlyElementsOf(objRefs.stream().map(ObjRef::id).toList());
  }
}
