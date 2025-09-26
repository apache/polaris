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
package org.apache.polaris.service.auth;

import io.smallrye.common.annotation.Identifier;
import jakarta.enterprise.context.RequestScoped;
import jakarta.inject.Inject;
import org.apache.polaris.core.auth.OpaPolarisAuthorizer;
import org.apache.polaris.core.auth.PolarisAuthorizer;
import org.apache.polaris.core.auth.PolarisAuthorizerFactory;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.service.config.AuthorizationConfiguration;

/** Factory for creating OPA-based Polaris authorizer implementations. */
@RequestScoped
@Identifier("opa")
public class OpaPolarisAuthorizerFactory implements PolarisAuthorizerFactory {

  private final AuthorizationConfiguration authorizationConfig;

  @Inject
  public OpaPolarisAuthorizerFactory(AuthorizationConfiguration authorizationConfig) {
    this.authorizationConfig = authorizationConfig;
  }

  @Override
  public PolarisAuthorizer create(RealmConfig realmConfig) {
    AuthorizationConfiguration.OpaConfig opa = authorizationConfig.opa();
    return OpaPolarisAuthorizer.create(
        opa.url().orElse(null),
        opa.policyPath().orElse(null),
        opa.timeoutMs().orElse(2000), // Default to 2000ms if not specified
        null,
        null);
  }
}
