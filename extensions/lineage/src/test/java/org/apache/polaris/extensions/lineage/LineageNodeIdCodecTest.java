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
package org.apache.polaris.extensions.lineage;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class LineageNodeIdCodecTest {

  @Test
  void encodesSimpleDatasetIdentityWithoutEscapes() {
    LineageDataset dataset = new LineageDataset("polaris", "analytics", "orders_daily");

    assertThat(LineageNodeIdCodec.encode(dataset))
        .isEqualTo("dataset:polaris:analytics.orders_daily");
  }

  @Test
  void roundTripsDelimiterBearingDatasetIdentity() {
    LineageDataset dataset = new LineageDataset("polaris:prod", "analytics.daily", "orders.daily");

    String nodeId = LineageNodeIdCodec.encode(dataset);

    assertThat(nodeId).isEqualTo("dataset:polaris%3Aprod:analytics%2Edaily.orders%2Edaily");
    assertThat(LineageNodeIdCodec.decode(nodeId)).contains(dataset);
  }

  @Test
  void roundTripsUnicodeDatasetIdentity() {
    LineageDataset dataset = new LineageDataset("日本", "販売 分析", "日次注文");

    assertThat(LineageNodeIdCodec.decode(LineageNodeIdCodec.encode(dataset))).contains(dataset);
  }

  @Test
  void rejectsMalformedNodeId() {
    assertThat(LineageNodeIdCodec.decode("table:polaris:analytics.orders")).isEmpty();
    assertThat(LineageNodeIdCodec.decode("dataset:polaris:analytics.orders%")).isEmpty();
    assertThat(LineageNodeIdCodec.decode("dataset:polaris:analytics.orders%ZZ")).isEmpty();
  }
}
