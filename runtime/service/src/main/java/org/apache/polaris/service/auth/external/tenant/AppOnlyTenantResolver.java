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
package org.apache.polaris.service.auth.external.tenant;

import io.quarkus.oidc.TenantResolver;
import io.vertx.ext.web.RoutingContext;
import jakarta.enterprise.context.ApplicationScoped;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

/**
 * Routes Azure AD app-only (managed identity) tokens to the {@value #SERVICE_TENANT} Quarkus OIDC
 * tenant, which maps the {@code azp} claim (the MI client ID) as the Polaris principal name.
 *
 * <p>Human delegated tokens contain {@code preferred_username} and are handled by the default
 * tenant ({@code polaris.oidc.principal-mapper.name-claim-path=preferred_username}). App-only
 * tokens issued via the client-credentials flow — including Azure managed identities — do not
 * include this claim and are routed to the service tenant instead
 * ({@code polaris.oidc.service.principal-mapper.name-claim-path=azp}).
 *
 * <p>The JWT payload is decoded without signature verification purely for routing. Full token
 * validation still occurs against the configured OIDC auth server for the selected tenant.
 */
@ApplicationScoped
public class AppOnlyTenantResolver implements TenantResolver {

  /** Quarkus OIDC tenant ID for app-only / managed-identity tokens. */
  public static final String SERVICE_TENANT = "service";

  @Override
  public String resolve(RoutingContext context) {
    String authHeader = context.request().getHeader("Authorization");
    if (authHeader == null || !authHeader.startsWith("Bearer ")) {
      return null;
    }
    String token = authHeader.substring(7);
    try {
      String[] parts = token.split("\\.");
      if (parts.length < 2) {
        return null;
      }
      byte[] decoded = Base64.getUrlDecoder().decode(padBase64(parts[1]));
      String payload = new String(decoded, StandardCharsets.UTF_8);
      // App-only tokens (managed identity / client-credentials) never contain
      // preferred_username. Route them to the service tenant for azp-based mapping.
      if (!payload.contains("\"preferred_username\"")) {
        return SERVICE_TENANT;
      }
    } catch (Exception ignored) {
      // Malformed token — fall through to default tenant (validation will reject it)
    }
    return null; // default tenant → preferred_username mapping for humans
  }

  private static String padBase64(String s) {
    int pad = s.length() % 4;
    if (pad == 2) return s + "==";
    if (pad == 3) return s + "=";
    return s;
  }
}
