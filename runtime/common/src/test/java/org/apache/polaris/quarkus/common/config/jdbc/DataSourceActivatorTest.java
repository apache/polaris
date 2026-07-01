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
package org.apache.polaris.quarkus.common.config.jdbc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.smallrye.config.PropertiesConfigSource;
import io.smallrye.config.SmallRyeConfig;
import io.smallrye.config.SmallRyeConfigBuilder;
import java.util.Map;
import org.junit.jupiter.api.Test;

class DataSourceActivatorTest {

  @Test
  void datasourceActivation() {
    SmallRyeConfig config =
        buildConfig(
            Map.of(
                "polaris.persistence.type", "relational-jdbc",
                "polaris.persistence.relational.jdbc.datasource", "mydb",
                "quarkus.datasource.mydb.active", "false",
                "quarkus.datasource.ds1.active", "true",
                "quarkus.datasource.ds2.active", "true",
                "quarkus.datasource.active", "true"));

    assertThat(isActive(config, "quarkus.datasource.mydb.active")).isTrue();
    assertThat(isActive(config, "quarkus.datasource.ds1.active")).isFalse();
    assertThat(isActive(config, "quarkus.datasource.ds2.active")).isFalse();
    assertThat(isActive(config, "quarkus.datasource.active")).isFalse();
  }

  @Test
  void nonJdbcPersistenceType() {
    SmallRyeConfig config =
        buildConfig(
            Map.of(
                "polaris.persistence.type", "in-memory",
                "quarkus.datasource.mydb.active", "true",
                "quarkus.datasource.ds1.active", "true",
                "quarkus.datasource.active", "true"));

    assertThat(isActive(config, "quarkus.datasource.mydb.active")).isFalse();
    assertThat(isActive(config, "quarkus.datasource.ds1.active")).isFalse();
    assertThat(isActive(config, "quarkus.datasource.active")).isFalse();
  }

  @Test
  void missingPersistenceType() {
    SmallRyeConfig config = buildConfig(Map.of("quarkus.datasource.mydb.active", "false"));

    assertThatThrownBy(() -> config.getConfigValue("quarkus.datasource.mydb.active").getValue())
        .hasMessage("missing required 'polaris.persistence.type' property in configuration");
  }

  @Test
  void missingTargetDatasourceName() {
    SmallRyeConfig config =
        buildConfig(
            Map.of(
                "polaris.persistence.type",
                "relational-jdbc",
                "quarkus.datasource.mydb.active",
                "false"));

    assertThatThrownBy(() -> config.getConfigValue("quarkus.datasource.mydb.active").getValue())
        .hasMessage(
            "missing required 'polaris.persistence.relational.jdbc.datasource' property in configuration");
  }

  @Test
  void wildcardActiveProperty() {
    SmallRyeConfig config =
        buildConfig(
            Map.of(
                "polaris.persistence.type", "relational-jdbc",
                "polaris.persistence.relational.jdbc.datasource", "mydb",
                "quarkus.datasource.*.active", "true"));

    // wildcard property should pass through unchanged
    assertThat(config.getConfigValue("quarkus.datasource.*.active").getValue()).isEqualTo("true");
  }

  @Test
  void quotedDatasourceNames() {
    SmallRyeConfig config =
        buildConfig(
            Map.of(
                "polaris.persistence.type", "\"relational-jdbc\"",
                "polaris.persistence.relational.jdbc.datasource", "\"my-db\"",
                "quarkus.datasource.\"my-db\".active", "false",
                "quarkus.datasource.\"other\".active", "true"));

    assertThat(isActive(config, "quarkus.datasource.my-db.active")).isTrue();
    assertThat(isActive(config, "quarkus.datasource.other.active")).isFalse();
  }

  private static SmallRyeConfig buildConfig(Map<String, String> properties) {
    return new SmallRyeConfigBuilder()
        .withInterceptors(new DataSourceActivator())
        .withSources(new PropertiesConfigSource(properties, "test", 100))
        .build();
  }

  private static boolean isActive(SmallRyeConfig config, String property) {
    return Boolean.parseBoolean(config.getConfigValue(property).getValue());
  }
}
