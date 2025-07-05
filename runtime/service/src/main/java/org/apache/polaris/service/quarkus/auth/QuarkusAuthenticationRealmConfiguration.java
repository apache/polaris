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
package org.apache.polaris.service.quarkus.auth;

import io.smallrye.config.WithDefault;
import java.time.Duration;
import org.apache.polaris.service.auth.AuthenticationRealmConfiguration;
import org.apache.polaris.service.auth.AuthenticationType;

public interface QuarkusAuthenticationRealmConfiguration extends AuthenticationRealmConfiguration {

  @WithDefault("internal")
  @Override
  AuthenticationType type();

  @Override
  QuarkusAuthenticatorConfiguration authenticator();

  @Override
  QuarkusActiveRolesProviderConfiguration activeRolesProvider();

  @Override
  QuarkusTokenServiceConfiguration tokenService();

  @Override
  QuarkusTokenBrokerConfiguration tokenBroker();

  interface QuarkusAuthenticatorConfiguration extends AuthenticatorConfiguration {

    /**
     * The type of the identity provider. Must be a registered {@link
     * org.apache.polaris.service.auth.Authenticator} identifier.
     */
    @WithDefault("default")
    String type();
  }

  interface QuarkusTokenServiceConfiguration extends TokenServiceConfiguration {
    /**
     * The type of the OAuth2 service. Must be a registered {@link
     * org.apache.polaris.service.catalog.api.IcebergRestOAuth2ApiService} identifier.
     */
    @WithDefault("default")
    String type();
  }

  interface QuarkusTokenBrokerConfiguration extends TokenBrokerConfiguration {

    @WithDefault("PT1H")
    @Override
    Duration maxTokenGeneration();

    /**
     * The type of the token broker factory. Must be a registered {@link
     * org.apache.polaris.service.auth.TokenBrokerFactory} identifier.
     */
    @WithDefault("rsa-key-pair")
    String type();
  }

  interface QuarkusActiveRolesProviderConfiguration extends ActiveRolesProviderConfiguration {

    /**
     * The type of the active roles provider. Must be a registered {@link
     * org.apache.polaris.service.auth.ActiveRolesProvider} identifier.
     */
    @WithDefault("default")
    String type();
  }
}
