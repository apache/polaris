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
 * Configuration interface for managing service identities across multiple realms in Polaris.
 *
 * <p>A service identity represents the Polaris service itself when it needs to authenticate to
 * external systems (e.g., AWS services for SigV4 authentication). Each realm can configure its own
 * set of service identities for different cloud providers.
 *
 * <p>This interface supports multi-tenant deployments where each realm (tenant) can have distinct
 * service identities, as well as single-tenant deployments with a default configuration shared
 * across all catalogs.
 *
 * <p>Configuration is loaded from {@code polaris.service-identity.*} properties at startup and
 * includes credentials that Polaris uses to assume customer-provided roles when accessing federated
 * catalogs.
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
   * Retrieves the configuration entry for the given realm context.
   *
   * <p>If the realm has no specific configuration, falls back to the default realm configuration.
   *
   * @param realmContext the realm context
   * @return the configuration entry containing the realm identifier and its configuration
   */
  default RealmConfigEntry forRealm(RealmContext realmContext) {
    return forRealm(realmContext.getRealmIdentifier());
  }

  /**
   * Retrieves the configuration entry for the given realm identifier.
   *
   * <p>If the realm has no specific configuration, falls back to the default realm configuration.
   *
   * @param realmIdentifier the realm identifier
   * @return the configuration entry containing the realm identifier and its configuration
   */
  default RealmConfigEntry forRealm(String realmIdentifier) {
    String resolvedRealmIdentifier =
        realms().containsKey(realmIdentifier) ? realmIdentifier : DEFAULT_REALM_KEY;
    return new RealmConfigEntry(resolvedRealmIdentifier, realms().get(resolvedRealmIdentifier));
  }

  /**
   * A pairing of a realm identifier and its associated service identity configuration.
   *
   * @param realm the realm identifier (may be the default if the requested realm was not
   *     configured)
   * @param config the service identity configuration for this realm
   */
  record RealmConfigEntry(String realm, RealmServiceIdentityConfiguration config) {}
}
