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
package org.apache.polaris.persistence.relational.jdbc.models;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class ModelRegistryTest {

  @Test
  void entityJsonColumnsAreRegistered() {
    assertThat(ModelRegistry.isJsonColumn(ModelEntity.TABLE_NAME, "properties")).isTrue();
    assertThat(ModelRegistry.isJsonColumn(ModelEntity.TABLE_NAME, "internal_properties")).isTrue();
  }

  @Test
  void eventJsonColumnsAreRegistered() {
    assertThat(ModelRegistry.isJsonColumn(ModelEvent.TABLE_NAME, "additional_properties")).isTrue();
  }

  @Test
  void policyMappingRecordJsonColumnsAreRegistered() {
    assertThat(ModelRegistry.isJsonColumn(ModelPolicyMappingRecord.TABLE_NAME, "parameters"))
        .isTrue();
  }

  @Test
  void metricsReportJsonColumnsAreRegistered() {
    assertThat(ModelRegistry.isJsonColumn(ModelCommitMetricsReport.TABLE_NAME, "metadata"))
        .isTrue();
    assertThat(ModelRegistry.isJsonColumn(ModelScanMetricsReport.TABLE_NAME, "metadata")).isTrue();
  }

  @Test
  void nonJsonColumnsReturnFalse() {
    assertThat(ModelRegistry.isJsonColumn(ModelEntity.TABLE_NAME, "id")).isFalse();
    assertThat(ModelRegistry.isJsonColumn(ModelEntity.TABLE_NAME, "name")).isFalse();
    assertThat(ModelRegistry.isJsonColumn(ModelPolicyMappingRecord.TABLE_NAME, "policy_id"))
        .isFalse();
  }

  @Test
  void unregisteredTableReturnsFalse() {
    assertThat(ModelRegistry.isJsonColumn("UNKNOWN_TABLE", "any_column")).isFalse();
  }
}
