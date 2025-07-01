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

package org.apache.polaris.core.storage.aws;

import org.apache.polaris.core.storage.StorageLocation;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

class S3LocationTest {
  @ParameterizedTest
  @ValueSource(strings = {"s3a", "s3"})
  public void testLocation(String scheme) {
    String locInput = scheme + "://bucket/schema1/table1";
    StorageLocation loc = StorageLocation.of(locInput);
    Assertions.assertThat(loc).isInstanceOf(S3Location.class);
    S3Location s3Loc = (S3Location) loc;
    Assertions.assertThat(s3Loc.getScheme()).isEqualTo(scheme);
    Assertions.assertThat(s3Loc.withoutScheme()).isEqualTo("//bucket/schema1/table1");
    Assertions.assertThat(s3Loc.withoutScheme()).doesNotStartWith(scheme);
    Assertions.assertThat(scheme + ":" + s3Loc.withoutScheme()).isEqualTo(locInput);
  }

  @ParameterizedTest
  @CsvSource({"s3,s3a", "s3a,s3"})
  public void testPrefixValidationIgnoresScheme(String parentScheme, String childScheme) {
    StorageLocation loc1 = StorageLocation.of(childScheme + "://bucket/schema1/table1");
    StorageLocation loc2 = StorageLocation.of(parentScheme + "://bucket/schema1");
    Assertions.assertThat(loc1.isChildOf(loc2)).isTrue();

    StorageLocation loc3 = StorageLocation.of(childScheme + "://bucket/schema1");
    Assertions.assertThat(loc2.equals(loc3)).isFalse();
  }
}
