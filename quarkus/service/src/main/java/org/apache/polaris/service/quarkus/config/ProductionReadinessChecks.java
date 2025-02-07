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
import org.apache.polaris.service.auth.AuthenticationConfiguration.TokenBrokerConfiguration.RSAKeyPairConfiguration;
import org.apache.polaris.service.auth.AuthenticationConfiguration.TokenBrokerConfiguration.SymmetricKeyConfiguration;
import org.apache.polaris.service.auth.Authenticator;
import org.apache.polaris.service.auth.JWTRSAKeyPairFactory;
import org.apache.polaris.service.auth.JWTSymmetricKeyFactory;
import org.apache.polaris.service.auth.TestInlineBearerTokenPolarisAuthenticator;
import org.apache.polaris.service.auth.TestOAuth2ApiService;
import org.apache.polaris.service.auth.TokenBrokerFactory;
import org.apache.polaris.service.catalog.api.IcebergRestOAuth2ApiService;
import org.apache.polaris.service.context.DefaultRealmContextResolver;
import org.apache.polaris.service.context.RealmContextResolver;
import org.apache.polaris.service.context.TestRealmContextResolver;
import org.apache.polaris.service.persistence.InMemoryPolarisMetaStoreManagerFactory;
import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.ConfigValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ApplicationScoped
public class ProductionReadinessChecks {

  private static final Logger LOGGER = LoggerFactory.getLogger(ProductionReadinessChecks.class);

  public void checkAuthenticator(
      @Observes StartupEvent event,
      Authenticator<String, AuthenticatedPolarisPrincipal> authenticator) {
    if (authenticator instanceof TestInlineBearerTokenPolarisAuthenticator) {
      warn(
          "The current authenticator is intended for tests only.",
          "polaris.authentication.authenticator.type");
    }
  }

  public void checkTokenService(@Observes StartupEvent event, IcebergRestOAuth2ApiService service) {
    if (service instanceof TestOAuth2ApiService) {
      warn(
          "The current token service is intended for tests only.",
          "polaris.authentication.token-service.type");
    }
  }

  public void checkTokenBroker(
      @Observes StartupEvent event,
      AuthenticationConfiguration configuration,
      TokenBrokerFactory factory) {
    if (factory instanceof JWTRSAKeyPairFactory) {
      if (configuration
          .tokenBroker()
          .rsaKeyPair()
          .map(RSAKeyPairConfiguration::publicKeyFile)
          .isEmpty()) {
        warn(
            "A public key file wasn't provided and will be generated.",
            "polaris.authentication.token-broker.rsa-key-pair.public-key-file");
      }
      if (configuration
          .tokenBroker()
          .rsaKeyPair()
          .map(RSAKeyPairConfiguration::privateKeyFile)
          .isEmpty()) {
        warn(
            "A private key file wasn't provided and will be generated.",
            "polaris.authentication.token-broker.rsa-key-pair.private-key-file");
      }
    }
    if (factory instanceof JWTSymmetricKeyFactory) {
      if (configuration
          .tokenBroker()
          .symmetricKey()
          .map(SymmetricKeyConfiguration::secret)
          .isPresent()) {
        warn(
            "A symmetric key secret was provided through configuration rather than through a secret file.",
            "polaris.authentication.token-broker.symmetric-key.secret");
      }
    }
  }

  public void checkMetastore(@Observes StartupEvent event, MetaStoreManagerFactory factory) {
    if (factory instanceof InMemoryPolarisMetaStoreManagerFactory) {
      warn("The current metastore is intended for tests only.", "polaris.persistence.type");
    }
  }

  public void checkRealmResolver(
      @Observes StartupEvent event, Config config, RealmContextResolver resolver) {
    if (resolver instanceof TestRealmContextResolver) {
      warn(
          "The current realm context resolver is intended for tests only.",
          "polaris.realm-context.type");
    }
    if (resolver instanceof DefaultRealmContextResolver) {
      ConfigValue configValue = config.getConfigValue("polaris.realm-context.require-header");
      boolean userProvided =
          configValue.getSourceOrdinal() > 250; // ordinal for application.properties in classpath
      if ("false".equals(configValue.getValue()) && !userProvided) {
        warn(
            "The realm context resolver is configured to map requests without a realm header to the default realm.",
            "polaris.realm-context.require-header");
      }
    }
  }

  private static void warn(String message, String property) {
    String fullMessage =
        "!!! {} Do not do this in production! Offending configuration option: '{}'. "
            + "See https://polaris.apache.org/in-dev/unreleased/configuring-polaris-for-production/.";
    LOGGER.warn(fullMessage, message, property);
  }
}
