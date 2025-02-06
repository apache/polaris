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
package org.apache.polaris.service.quarkus.config;

import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import org.apache.polaris.core.auth.AuthenticatedPolarisPrincipal;
import org.apache.polaris.core.persistence.MetaStoreManagerFactory;
import org.apache.polaris.service.auth.AuthenticationConfiguration;
import org.apache.polaris.service.auth.AuthenticationConfiguration.TokenBrokerConfiguration.SymmetricKeyConfiguration;
import org.apache.polaris.service.auth.Authenticator;
import org.apache.polaris.service.auth.JWTRSAKeyPairFactory;
import org.apache.polaris.service.auth.JWTSymmetricKeyFactory;
import org.apache.polaris.service.auth.TestInlineBearerTokenPolarisAuthenticator;
import org.apache.polaris.service.auth.TestOAuth2ApiService;
import org.apache.polaris.service.auth.TokenBrokerFactory;
import org.apache.polaris.service.catalog.api.IcebergRestOAuth2ApiService;
import org.apache.polaris.service.context.DefaultRealmContextResolver;
import org.apache.polaris.service.context.RealmContextConfiguration;
import org.apache.polaris.service.context.RealmContextResolver;
import org.apache.polaris.service.context.TestRealmContextResolver;
import org.apache.polaris.service.persistence.InMemoryPolarisMetaStoreManagerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ApplicationScoped
public class ProductionReadinessChecks {

  private static final Logger LOGGER = LoggerFactory.getLogger(ProductionReadinessChecks.class);

  public void checkAuthenticator(
      @Observes StartupEvent event,
      Authenticator<String, AuthenticatedPolarisPrincipal> authenticator) {
    if (authenticator instanceof TestInlineBearerTokenPolarisAuthenticator) {
      warnOnTestOnly("authenticator", "polaris.authentication.authenticator.type");
    }
  }

  public void checkTokenService(@Observes StartupEvent event, IcebergRestOAuth2ApiService service) {
    if (service instanceof TestOAuth2ApiService) {
      warnOnTestOnly("token service", "polaris.authentication.token-service.type");
    }
  }

  public void checkTokenBroker(
      @Observes StartupEvent event,
      AuthenticationConfiguration configuration,
      TokenBrokerFactory factory) {
    if (factory instanceof JWTRSAKeyPairFactory) {
      LOGGER.warn(
          "No public and private key files were provided; these will be generated. "
              + "Do not do this in production (offending configuration options: "
              + "'polaris.authentication.token-broker.rsa-key-pair.public-key-file' and "
              + "'polaris.authentication.token-broker.rsa-key-pair.private-key-file').");
    }
    if (factory instanceof JWTSymmetricKeyFactory) {
      if (configuration
          .tokenBroker()
          .symmetricKey()
          .map(SymmetricKeyConfiguration::secret)
          .isPresent()) {
        LOGGER.warn(
            "A symmetric key secret was provided through configuration rather than through a secret file. "
                + "Do not do this in production (offending configuration option: "
                + "'polaris.authentication.token-broker.symmetric-key.secret').");
      }
    }
  }

  public void checkMetastore(@Observes StartupEvent event, MetaStoreManagerFactory factory) {
    if (factory instanceof InMemoryPolarisMetaStoreManagerFactory) {
      warnOnTestOnly("metastore", "polaris.persistence.type");
    }
  }

  public void checkRealmResolver(
      @Observes StartupEvent event,
      RealmContextConfiguration configuration,
      RealmContextResolver resolver) {
    if (resolver instanceof TestRealmContextResolver) {
      warnOnTestOnly("realm context resolver", "polaris.realm-context.type");
    }
    if (resolver instanceof DefaultRealmContextResolver) {
      if (!configuration.requireHeader()) {
        LOGGER.warn(
            "The realm context resolver is configured to map requests without a realm header to the default realm. "
                + "Do not do this in production (offending configuration option: 'polaris.realm-context.require-header').");
      }
    }
  }

  private static void warnOnTestOnly(String description, String property) {
    LOGGER.warn(
        "The current {} is intended for tests only. Do not use it in production (offending configuration option: '{}').",
        description,
        property);
  }
}
