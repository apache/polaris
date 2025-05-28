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
package org.apache.polaris.service.quarkus.config;

import io.smallrye.config.ConfigMapping;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.polaris.core.config.PolarisConfiguration;
import org.apache.polaris.service.config.ReservedProperties;

@ConfigMapping(prefix = "polaris.reserved-properties")
public interface QuarkusReservedProperties extends ReservedProperties {
  @Override
  default List<String> prefixes() {
    return List.of("polaris.");
  }

  @Override
  default Set<String> allowlist() {
    return AllowlistHolder.INSTANCE;
  }

  class AllowlistHolder {
    static final Set<String> INSTANCE = computeAllowlist();

    private static Set<String> computeAllowlist() {
      Set<String> allowlist = new HashSet<>();
      PolarisConfiguration.getAllConfigurations()
          .forEach(
              c -> {
                if (c.hasCatalogConfig()) {
                  allowlist.add(c.catalogConfig());
                }
                if (c.hasCatalogConfigUnsafe()) {
                  allowlist.add(c.catalogConfigUnsafe());
                }
              });
      return allowlist;
    }
  }
}
