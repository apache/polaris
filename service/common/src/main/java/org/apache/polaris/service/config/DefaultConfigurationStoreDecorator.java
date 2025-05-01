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

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import jakarta.annotation.Priority;
import jakarta.decorator.Decorator;
import jakarta.decorator.Delegate;
import jakarta.inject.Inject;
import java.util.Map;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.config.PolarisConfigurationStore;
import org.apache.polaris.core.context.CallContext;

@Decorator
@Priority(1)
public class DefaultConfigurationStoreDecorator implements PolarisConfigurationStore {
  @Inject @Delegate PolarisConfigurationStore delegate;

  private final Map<String, Object> defaults;
  private final Map<String, Map<String, Object>> realmOverrides;
  private CallContext callContext;

  @Inject
  public DefaultConfigurationStoreDecorator(
      ObjectMapper objectMapper, FeaturesConfiguration configurations, CallContext callContext) {
    this(
        configurations.parseDefaults(objectMapper),
        configurations.parseRealmOverrides(objectMapper),
        callContext);
  }

  public DefaultConfigurationStoreDecorator(Map<String, Object> defaults, CallContext callContext) {
    this(defaults, Map.of(), callContext);
  }

  public DefaultConfigurationStoreDecorator(
      Map<String, Object> defaults,
      Map<String, Map<String, Object>> realmOverrides,
      CallContext callContext) {
    this.defaults = Map.copyOf(defaults);
    this.realmOverrides = Map.copyOf(realmOverrides);
    this.callContext = callContext;
  }

  @Override
  public <T> @Nullable T getConfiguration(@Nonnull PolarisCallContext ctx, String configName) {
    if (callContext == null) {
      return delegate.getConfiguration(ctx, configName);
    } else {
      String realm = callContext.getRealmContext().getRealmIdentifier();
      @SuppressWarnings("unchecked")
      T confgValue =
          (T)
              realmOverrides
                  .getOrDefault(realm, Map.of())
                  .getOrDefault(configName, defaults.get(configName));
      return confgValue;
    }
  }
}
