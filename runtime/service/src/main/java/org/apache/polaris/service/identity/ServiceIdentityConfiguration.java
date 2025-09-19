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

package org.apache.polaris.service.identity;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefaults;
import io.smallrye.config.WithParentName;
import io.smallrye.config.WithUnnamedKey;
import java.util.Map;
import org.apache.polaris.core.context.RealmContext;

/**
 * Represents the service identity configuration for one or more realms.
 *
 * <p>This interface supports multi-tenant configurations where each realm can define its own {@link
 * RealmServiceIdentityConfiguration}. If a realm-specific configuration is not found, a fallback to
 * the default configuration is applied.
 */
@ConfigMapping(prefix = "polaris.service-identity")
public interface ServiceIdentityConfiguration {
  /**
   * The key used to identify the default realm configuration.
   *
   * <p>This default is especially useful in testing scenarios and single-tenant deployments where
   * only one realm is expected and explicitly configuring realms is unnecessary.
   */
  String DEFAULT_REALM_KEY = "<default>";

  /**
   * Returns a map of realm identifiers to their corresponding service identity configurations.
   *
   * @return the map of realm-specific configurations
   */
  @WithParentName
  @WithUnnamedKey(DEFAULT_REALM_KEY)
  @WithDefaults
  Map<String, RealmServiceIdentityConfiguration> realms();

  /**
   * Returns the service identity configuration for the given {@link RealmContext}. Falls back to
   * the default if the realm is not explicitly configured.
   *
   * @param realmContext the realm context
   * @return the matching or default realm configuration
   */
  default RealmServiceIdentityConfiguration forRealm(RealmContext realmContext) {
    return forRealm(realmContext.getRealmIdentifier());
  }

  /**
   * Returns the service identity configuration for the given realm identifier. Falls back to the
   * default if the realm is not explicitly configured.
   *
   * @param realmIdentifier the identifier of the realm
   * @return the matching or default realm configuration
   */
  default RealmServiceIdentityConfiguration forRealm(String realmIdentifier) {
    return realms().containsKey(realmIdentifier)
        ? realms().get(realmIdentifier)
        : realms().get(DEFAULT_REALM_KEY);
  }

  /**
   * Returns the actual key of the service identity configuration to use for the given {@link
   * RealmContext}, falling back to the default if the specified realm is not configured.
   *
   * @param realmContext the realm context
   * @return the actual realm identifier to use
   */
  default String resolveRealm(RealmContext realmContext) {
    return resolveRealm(realmContext.getRealmIdentifier());
  }

  /**
   * Returns the actual key of the service identity configuration to use for the given realm
   * identifier, falling back to the default if the specified realm is not configured.
   *
   * @param realmIdentifier the identifier of the realm
   * @return the actual realm identifier to use
   */
  default String resolveRealm(String realmIdentifier) {
    return realms().containsKey(realmIdentifier) ? realmIdentifier : DEFAULT_REALM_KEY;
  }
}
