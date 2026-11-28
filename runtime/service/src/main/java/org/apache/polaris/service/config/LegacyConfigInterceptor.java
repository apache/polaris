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

import io.smallrye.config.FallbackConfigSourceInterceptor;
import java.util.Map;

/**
 * Provides fallback mappings from legacy config property names to their current replacements. When
 * a current property is not set, its value is transparently read from the legacy property.
 */
public class LegacyConfigInterceptor extends FallbackConfigSourceInterceptor {

  private static final Map<String, String> FALLBACKS =
      Map.of(
          "polaris.event-listener.types", "polaris.event-listener.type",
          "polaris.authorization.authorizer.type", "polaris.authorization.type",
          // The authorizer type is optional, so it is only read if its name is listed by
          // iterateNames(), which applies the mappings to the names that are set: the reverse
          // mapping lists the current name when only the legacy one is set.
          "polaris.authorization.type", "polaris.authorization.authorizer.type");

  public LegacyConfigInterceptor() {
    super(FALLBACKS);
  }
}
