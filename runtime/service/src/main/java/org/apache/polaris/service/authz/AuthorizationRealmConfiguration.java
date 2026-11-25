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
package org.apache.polaris.service.authz;

import java.util.Optional;

public interface AuthorizationRealmConfiguration {

  String DEFAULT_TYPE = "internal";

  /**
   * The type of the authorizer. Must be a registered {@link
   * org.apache.polaris.core.auth.PolarisAuthorizerFactory} identifier. Defaults to {@code
   * internal}.
   *
   * <p>For the default realm, falls back to the legacy {@code polaris.authorization.type} property
   * if this property is not set. A realm that sets a {@code tenant-key} must also set its {@code
   * type} explicitly.
   */
  Optional<String> type();

  /** Returns the {@link #type()} of the authorizer, or the default type if not set. */
  default String effectiveType() {
    return type().orElse(DEFAULT_TYPE);
  }

  /**
   * The authorizer tenant key identifying which authorizer configuration to use. When present,
   * refers to a named entry in the authorizer-specific tenant configuration (e.g. {@code
   * polaris.authorization.opa.<tenant-key>.*}). When absent, the default (unnamed) configuration is
   * used.
   */
  Optional<String> tenantKey();
}
