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

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.event.Startup;
import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.Produces;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.apache.polaris.core.config.ProductionReadinessCheck;
import org.apache.polaris.core.config.ProductionReadinessCheck.Error;
import org.apache.polaris.core.persistence.MetaStoreManagerFactory;
import org.apache.polaris.service.auth.AuthenticationRealmConfiguration.TokenBrokerConfiguration.RSAKeyPairConfiguration;
import org.apache.polaris.service.auth.AuthenticationRealmConfiguration.TokenBrokerConfiguration.SymmetricKeyConfiguration;
import org.apache.polaris.service.auth.AuthenticationType;
import org.apache.polaris.service.context.DefaultRealmContextResolver;
import org.apache.polaris.service.context.RealmContextResolver;
import org.apache.polaris.service.context.TestRealmContextResolver;
import org.apache.polaris.service.events.PolarisEventListener;
import org.apache.polaris.service.events.TestPolarisEventListener;
import org.apache.polaris.service.persistence.InMemoryPolarisMetaStoreManagerFactory;
import org.apache.polaris.service.quarkus.auth.QuarkusAuthenticationConfiguration;
import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.ConfigValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ApplicationScoped
public class ProductionReadinessChecks {

  private static final Logger LOGGER = LoggerFactory.getLogger(ProductionReadinessChecks.class);

  /**
   * A warning sign ⚠ {@code 26A0} with variant selector {@code FE0F}. The sign is preceded by a
   * null character {@code 0000} to ensure that the warning sign is displayed correctly regardless
   * of the log pattern (some log patterns seem to interfere with non-ASCII characters).
   */
  private static final String WARNING_SIGN_UTF_8 = "\u0000\u26A0\uFE0F";

  /** A simple warning sign displayed when the character set is not UTF-8. */
  private static final String WARNING_SIGN_PLAIN = "!!!";

  public void warnOnFailedChecks(
      @Observes Startup event, Instance<ProductionReadinessCheck> checks) {
    List<Error> errors = checks.stream().flatMap(check -> check.getErrors().stream()).toList();
    if (!errors.isEmpty()) {
      String warning =
          Charset.defaultCharset().equals(StandardCharsets.UTF_8)
              ? WARNING_SIGN_UTF_8
              : WARNING_SIGN_PLAIN;
      LOGGER.warn("{} Production readiness checks failed! Check the warnings below.", warning);
      errors.forEach(
          error ->
              LOGGER.warn(
                  "- {} Offending configuration option: '{}'.",
                  error.message(),
                  error.offendingProperty()));
      LOGGER.warn(
          "Refer to https://polaris.apache.org/in-dev/unreleased/configuring-polaris-for-production for more information.");
    }
  }

  @Produces
  public ProductionReadinessCheck checkTokenBrokers(
      QuarkusAuthenticationConfiguration configuration) {
    List<ProductionReadinessCheck.Error> errors = new ArrayList<>();
    configuration
        .realms()
        .forEach(
            (realm, config) -> {
              if (config.type() != AuthenticationType.EXTERNAL) {
                if (config.tokenBroker().type().equals("rsa-key-pair")) {
                  if (config
                      .tokenBroker()
                      .rsaKeyPair()
                      .map(RSAKeyPairConfiguration::publicKeyFile)
                      .isEmpty()) {
                    errors.add(
                        Error.of(
                            "A public key file wasn't provided and will be generated.",
                            "polaris.authentication.%stoken-broker.rsa-key-pair.public-key-file"
                                .formatted(authRealmSegment(realm))));
                  }
                  if (config
                      .tokenBroker()
                      .rsaKeyPair()
                      .map(RSAKeyPairConfiguration::privateKeyFile)
                      .isEmpty()) {
                    errors.add(
                        Error.of(
                            "A private key file wasn't provided and will be generated.",
                            "polaris.authentication.%stoken-broker.rsa-key-pair.private-key-file"
                                .formatted(authRealmSegment(realm))));
                  }
                }
                if (config.tokenBroker().type().equals("symmetric-key")) {
                  if (config
                      .tokenBroker()
                      .symmetricKey()
                      .map(SymmetricKeyConfiguration::secret)
                      .isPresent()) {
                    errors.add(
                        Error.of(
                            "A symmetric key secret was provided through configuration rather than through a secret file.",
                            "polaris.authentication.%stoken-broker.symmetric-key.secret"
                                .formatted(authRealmSegment(realm))));
                  }
                }
              }
            });
    return ProductionReadinessCheck.of(errors);
  }

  @Produces
  public ProductionReadinessCheck checkMetastore(MetaStoreManagerFactory factory) {
    if (factory instanceof InMemoryPolarisMetaStoreManagerFactory) {
      return ProductionReadinessCheck.of(
          Error.of(
              "The current metastore is intended for tests only.", "polaris.persistence.type"));
    }
    return ProductionReadinessCheck.OK;
  }

  @Produces
  public ProductionReadinessCheck checkRealmResolver(Config config, RealmContextResolver resolver) {
    if (resolver instanceof TestRealmContextResolver) {
      return ProductionReadinessCheck.of(
          Error.of(
              "The current realm context resolver is intended for tests only.",
              "polaris.realm-context.type"));
    }
    if (resolver instanceof DefaultRealmContextResolver) {
      ConfigValue configValue = config.getConfigValue("polaris.realm-context.require-header");
      boolean userProvided =
          configValue.getSourceOrdinal() > 250; // ordinal for application.properties in classpath
      if ("false".equals(configValue.getValue()) && !userProvided) {
        return ProductionReadinessCheck.of(
            Error.of(
                "The realm context resolver is configured to map requests without a realm header to the default realm.",
                "polaris.realm-context.require-header"));
      }
    }
    return ProductionReadinessCheck.OK;
  }

  @Produces
  public ProductionReadinessCheck checkPolarisEventListener(
      PolarisEventListener polarisEventListener) {
    if (polarisEventListener instanceof TestPolarisEventListener) {
      return ProductionReadinessCheck.of(
          Error.of("TestPolarisEventListener is intended for tests only.", "polaris.events.type"));
    }
    return ProductionReadinessCheck.OK;
  }

  private static String authRealmSegment(String realm) {
    return realm.equals(QuarkusAuthenticationConfiguration.DEFAULT_REALM_KEY) ? "" : realm + ".";
  }
}
