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
package org.apache.polaris.service.storage;

import java.util.List;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.PolarisConfiguration;
import org.apache.polaris.core.PolarisConfigurationStore;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Unit test for the default behaviors of the PolarisConfigurationStore interface. */
public class PolarisConfigurationStoreTest {
  @Test
  public void testConfigsCanBeCastedFromString() {
    List<PolarisConfiguration<?>> configs =
        List.of(
            buildConfig("bool", true),
            buildConfig("short", (short) 5),
            buildConfig("int", 5),
            buildConfig("long", 5L),
            buildConfig("float", 5F),
            buildConfig("double", 5D));

    PolarisConfigurationStore store =
        new PolarisConfigurationStore() {
          @SuppressWarnings("unchecked")
          @Override
          public <T> @Nullable T getConfiguration(PolarisCallContext ctx, String configName) {
            for (PolarisConfiguration<?> c : configs) {
              if (c.key.equals(configName)) {
                return (T) String.valueOf(c.defaultValue);
              }
            }

            throw new IllegalStateException(
                String.format(
                    "Didn't find config value for %s, the test isn't set up incorrectly",
                    configName));
          }
        };

    for (PolarisConfiguration<?> c : configs) {
      Assertions.assertEquals(c.defaultValue, store.getConfiguration(null, c));
    }
  }

  private static <T> PolarisConfiguration<T> buildConfig(String key, T defaultValue) {
    return PolarisConfiguration.<T>builder()
        .key(key)
        .description("")
        .defaultValue(defaultValue)
        .build();
  }
}
