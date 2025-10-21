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
import org.apache.polaris.core.auth.PolarisAuthorizerFactory;
import org.apache.polaris.core.config.ProductionReadinessCheck;
import org.apache.polaris.core.config.ProductionReadinessCheck.Error;

@ApplicationScoped
public class OpaProductionReadinessChecks {

  @Produces
  public ProductionReadinessCheck checkOpaAuthorization(
      PolarisAuthorizerFactory authorizerFactory, OpaAuthorizationConfig config) {
    if (authorizerFactory instanceof OpaPolarisAuthorizerFactory) {
      List<Error> errors = new ArrayList<>();

      errors.add(
          Error.of(
              "OPA authorization is currently a Beta feature and is not a stable release. Breaking changes may be introduced in future versions. Use with caution in production environments.",
              "polaris.authorization.type"));

      if (!config.http().verifySsl()) {
        errors.add(
            Error.ofSevere(
                "SSL certificate verification is disabled for OPA communication. This exposes the service to man-in-the-middle attacks and other severe security risks.",
                "polaris.authorization.opa.http.verify-ssl"));
      }

      return ProductionReadinessCheck.of(errors);
    }
    return ProductionReadinessCheck.OK;
  }
}
