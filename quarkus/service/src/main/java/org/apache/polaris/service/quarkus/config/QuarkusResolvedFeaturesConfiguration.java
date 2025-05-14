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

import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Alternative;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.polaris.service.config.FeaturesConfiguration;

/**
 * Wraps around {@link QuarkusFeaturesConfiguration} but removes properties from `defaults` that
 * shouldn't be there
 */
@ApplicationScoped
@Alternative
@Priority(1)
public class QuarkusResolvedFeaturesConfiguration implements FeaturesConfiguration {

  private final Map<String, String> cleanedDefaults;
  private final Map<String, ? extends RealmOverrides> realmOverrides;

  public QuarkusResolvedFeaturesConfiguration(QuarkusFeaturesConfiguration raw) {
    this.realmOverrides = raw.realmOverrides();

    // Filter out any keys that look like realm overrides
    this.cleanedDefaults =
        raw.defaults().entrySet().stream()
            .filter(e -> e.getKey().split("\\.").length == 1)
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  @Override
  public Map<String, String> defaults() {
    return cleanedDefaults;
  }

  @Override
  public Map<String, ? extends RealmOverrides> realmOverrides() {
    return realmOverrides;
  }
}
