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
package org.apache.polaris.service.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.polaris.core.config.ProductionReadinessCheck;
import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.ConfigValue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ProductionReadinessChecksTest {

  private static final String REFLECTION_FREE_SERIALIZERS_PROPERTY =
      "quarkus.rest.jackson.optimization.enable-reflection-free-serializers";

  private ProductionReadinessChecks checks;

  @BeforeEach
  void setUp() {
    checks = new ProductionReadinessChecks();
  }

  @Test
  void reflectionFreeSerializersDisabledReturnsOk() {
    ProductionReadinessCheck result =
        checks.checkReflectionFreeSerializers(configWithReflectionFreeSerializers("false"));

    assertThat(result.ready()).isTrue();
  }

  @Test
  void reflectionFreeSerializersUnsetReturnsOk() {
    ProductionReadinessCheck result =
        checks.checkReflectionFreeSerializers(configWithReflectionFreeSerializers(null));

    assertThat(result.ready()).isTrue();
  }

  @Test
  void reflectionFreeSerializersEnabledReturnsSevereError() {
    ProductionReadinessCheck result =
        checks.checkReflectionFreeSerializers(configWithReflectionFreeSerializers("true"));

    assertThat(result.ready()).isFalse();
    assertThat(result.getErrors())
        .singleElement()
        .satisfies(
            error -> {
              assertThat(error.offendingProperty()).isEqualTo(REFLECTION_FREE_SERIALIZERS_PROPERTY);
              assertThat(error.severe()).isTrue();
            });
  }

  private static Config configWithReflectionFreeSerializers(String value) {
    Config config = mock(Config.class);
    ConfigValue configValue = mock(ConfigValue.class);
    when(config.getConfigValue(REFLECTION_FREE_SERIALIZERS_PROPERTY)).thenReturn(configValue);
    when(configValue.getValue()).thenReturn(value);
    return config;
  }
}
