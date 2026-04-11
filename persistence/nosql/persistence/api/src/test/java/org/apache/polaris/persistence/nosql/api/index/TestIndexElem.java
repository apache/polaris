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

package org.apache.polaris.persistence.nosql.api.index;

import static org.assertj.core.api.Assertions.assertThat;

import org.jspecify.annotations.NonNull;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class TestIndexElem {

  @ParameterizedTest
  @CsvSource({
    "v1, v1, true",
    "v1, v2, false",
    "v1,   , false" // v2 is null
  })
  void testEquality(String v1, String v2, boolean equal) {
    IndexKey k = IndexKey.key("test");
    Index.Element<String> e1 = IndexElem.of(k, v1);
    Index.Element<String> e2 = new OtherElem(k, v2);
    assertThat(e1.equals(e2)).isEqualTo(equal);
    assertThat(e2.equals(e1)).isEqualTo(equal);
  }

  @Test
  void testHashCode() {
    IndexKey k = IndexKey.key("test");
    assertThat(IndexElem.of(k, "v")).hasSameHashCodeAs(new OtherElem(k, "v"));
    assertThat(new OtherElem(k, null)).hasSameHashCodeAs(new OtherElem(k, null));
  }

  private static class OtherElem extends IndexElem<String> {
    private final String value;
    private final IndexKey key;

    private OtherElem(IndexKey key, String value) {
      this.value = value;
      this.key = key;
    }

    @Override
    protected String valueNullable() {
      return value;
    }

    @Override
    @NonNull
    public IndexKey key() {
      return key;
    }
  }
}
