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
package org.apache.polaris.service.catalog.iceberg;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import org.junit.jupiter.api.Test;

class CapturedConfigHolderTest {

  @Test
  void emptyByDefault() {
    CapturedConfigHolder holder = new CapturedConfigHolder();
    assertThat(holder.getTableId()).isEmpty();
  }

  @Test
  void capturesTableId() {
    CapturedConfigHolder holder = new CapturedConfigHolder();
    holder.setCapturedConfig(
        Map.of(
            "tableId", "73031312-6e6f-432c-ab35-ae23561f6472",
            "tableBucketId", "d7290d06-163c-4d10-a193-8d920e9a0cf0"));
    assertThat(holder.getTableId()).hasValue("73031312-6e6f-432c-ab35-ae23561f6472");
  }

  @Test
  void returnsEmptyWhenNoTableIdInConfig() {
    CapturedConfigHolder holder = new CapturedConfigHolder();
    holder.setCapturedConfig(Map.of("tableBucketId", "d7290d06"));
    assertThat(holder.getTableId()).isEmpty();
  }

  @Test
  void clearResetsState() {
    CapturedConfigHolder holder = new CapturedConfigHolder();
    holder.setCapturedConfig(Map.of("tableId", "abc123"));
    assertThat(holder.getTableId()).hasValue("abc123");
    holder.clear();
    assertThat(holder.getTableId()).isEmpty();
  }
}
