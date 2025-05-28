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
package org.apache.polaris.service.quarkus.auth.external.tenant;

import io.quarkus.oidc.runtime.OidcUtils;
import io.quarkus.security.identity.SecurityIdentity;
import io.smallrye.common.annotation.Identifier;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.polaris.service.quarkus.auth.external.OidcConfiguration;
import org.apache.polaris.service.quarkus.auth.external.OidcTenantConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default OIDC tenant configuration resolver.
 *
 * <p>First, it locates the Quarkus OIDC tenant in use based on the identity attributes; then it
 * matches the Polaris OIDC tenant configuration by tenant ID. If no matching tenant is found, it
 * falls back to the default tenant configuration.
 */
@ApplicationScoped
@Identifier("default")
class DefaultOidcTenantResolver implements OidcTenantResolver {

  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultOidcTenantResolver.class);

  private final OidcConfiguration config;

  @Inject
  public DefaultOidcTenantResolver(OidcConfiguration config) {
    this.config = config;
  }

  @Override
  public OidcTenantConfiguration resolveConfig(SecurityIdentity identity) {
    var tenantConfig = config.tenants().get(OidcConfiguration.DEFAULT_TENANT_KEY);
    String tenantId = identity.getAttribute(OidcUtils.TENANT_ID_ATTRIBUTE);
    if (tenantId != null && !tenantId.equals(OidcUtils.DEFAULT_TENANT_ID)) {
      if (config.tenants().containsKey(tenantId)) {
        tenantConfig = config.tenants().get(tenantId);
      } else {
        LOGGER.warn(
            "Quarkus OIDC tenant {} not found in Polaris OIDC configuration, "
                + "using default Polaris OIDC configuration instead",
            tenantId);
      }
    }
    return tenantConfig;
  }
}
