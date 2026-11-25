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
package org.apache.polaris.extension.auth.opa;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.polaris.core.config.ProductionReadinessCheck;
import org.apache.polaris.core.config.ProductionReadinessCheck.Error;

@ApplicationScoped
public class OpaProductionReadinessChecks {

  @Produces
  public ProductionReadinessCheck checkOpaAuthorization(OpaAuthorizationConfig opaConfig) {
    List<Error> errors = new ArrayList<>();
    boolean anyOpaConfigured = false;

    for (Map.Entry<String, OpaAuthorizationConfig.OpaAuthorizationTenantConfig> entry :
        opaConfig.tenants().entrySet()) {
      OpaAuthorizationConfig.OpaAuthorizationTenantConfig tenantConfig = entry.getValue();
      if (tenantConfig.policyUri().isEmpty()) {
        continue;
      }
      anyOpaConfigured = true;

      if (!tenantConfig.http().verifySsl()) {
        String tenantKey = entry.getKey();
        String propPrefix =
            OpaAuthorizationConfig.DEFAULT_TENANT_KEY.equals(tenantKey)
                ? "polaris.authorization.opa"
                : "polaris.authorization.opa." + tenantKey;
        errors.add(
            Error.ofSevere(
                "SSL certificate verification is disabled for OPA communication. This exposes the service to man-in-the-middle attacks and other severe security risks.",
                propPrefix + ".http.verify-ssl"));
      }
    }

    if (anyOpaConfigured) {
      errors.add(
          0,
          Error.of(
              "OPA authorization is currently a Beta feature and is not a stable release. Breaking changes may be introduced in future versions. Use with caution in production environments.",
              "polaris.authorization.authorizer.type"));
    }

    return ProductionReadinessCheck.of(errors);
  }
}
