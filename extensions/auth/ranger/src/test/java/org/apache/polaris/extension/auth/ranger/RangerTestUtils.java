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

package org.apache.polaris.extension.auth.ranger;

import java.util.Map;
import java.util.Optional;
import org.apache.polaris.core.config.PolarisConfiguration;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.entity.CatalogEntity;

public class RangerTestUtils {
  public static RangerPolarisAuthorizerConfig createConfig(String configFilename) {
    return new RangerPolarisAuthorizerConfig() {
      @Override
      public Optional<String> configFileName() {
        return Optional.of(configFilename);
      }
    };
  }

  public static RealmConfig createRealmConfig() {
    return new RealmConfig() {
      @SuppressWarnings({"removal"})
      @Override
      public <T> T getConfig(String configName) {
        return null;
      }

      @SuppressWarnings({"removal"})
      @Override
      public <T> T getConfig(String configName, T defaultValue) {
        return null;
      }

      @Override
      public <T> T getConfig(PolarisConfiguration<T> config) {
        return config.defaultValue();
      }

      @Override
      public <T> T getConfig(PolarisConfiguration<T> config, CatalogEntity catalogEntity) {
        return config.defaultValue();
      }

      @Override
      public <T> T getConfig(
          PolarisConfiguration<T> config, Map<String, String> catalogProperties) {
        return config.defaultValue();
      }
    };
  }
}
