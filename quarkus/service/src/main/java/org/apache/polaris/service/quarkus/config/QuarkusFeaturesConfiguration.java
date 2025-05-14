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
import io.smallrye.config.WithParentName;
import java.util.Map;
import org.apache.polaris.service.config.FeaturesConfiguration;

@ConfigMapping(prefix = "polaris.features")
public interface QuarkusFeaturesConfiguration extends FeaturesConfiguration {

  @WithParentName
  @Override
  Map<String, String> defaults();

  @Override
  Map<String, QuarkusRealmOverrides> realmOverrides();

  interface QuarkusRealmOverrides extends RealmOverrides {
    @WithParentName
    @Override
    Map<String, String> overrides();
  }
}
