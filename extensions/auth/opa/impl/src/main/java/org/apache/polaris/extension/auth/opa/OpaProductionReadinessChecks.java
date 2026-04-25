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

import io.smallrye.common.annotation.Identifier;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.Produces;
import java.util.ArrayList;
import java.util.List;
import org.apache.polaris.core.config.ProductionReadinessCheck;
import org.apache.polaris.core.config.ProductionReadinessCheck.Error;
import org.eclipse.microprofile.config.Config;

@ApplicationScoped
public class OpaProductionReadinessChecks {

  private static final String OPA_AUTHORIZATION_TYPE = "opa";
  private static final String DEFAULT_AUTHORIZATION_TYPE_PROPERTY = "polaris.authorization.type";
  private static final String AUTHORIZATION_REALMS_PREFIX = "polaris.authorization.realms.";
  private static final String AUTHORIZATION_TYPE_SUFFIX = ".type";

  @Produces
  public ProductionReadinessCheck checkOpaAuthorization(
      Config config, @Any Instance<OpaPolarisAuthorizerFactory> opaAuthorizerFactories) {
    List<String> opaAuthorizationTypeProperties = opaAuthorizationTypeProperties(config);
    if (opaAuthorizationTypeProperties.isEmpty()) {
      return ProductionReadinessCheck.OK;
    }

    Instance<OpaPolarisAuthorizerFactory> selectedFactory =
        opaAuthorizerFactories.select(Identifier.Literal.of(OPA_AUTHORIZATION_TYPE));
    if (!selectedFactory.isResolvable()) {
      return ProductionReadinessCheck.OK;
    }

    OpaAuthorizationConfig opaConfig = selectedFactory.get().getConfig();
    List<Error> errors = new ArrayList<>();

    for (String property : opaAuthorizationTypeProperties) {
      errors.add(
          Error.of(
              "OPA authorization is currently a Beta feature and is not a stable release. Breaking changes may be introduced in future versions. Use with caution in production environments.",
              property));
    }

    if (!opaConfig.http().verifySsl()) {
      errors.add(
          Error.ofSevere(
              "SSL certificate verification is disabled for OPA communication. This exposes the service to man-in-the-middle attacks and other severe security risks.",
              "polaris.authorization.opa.http.verify-ssl"));
    }

    return ProductionReadinessCheck.of(errors);
  }

  private static List<String> opaAuthorizationTypeProperties(Config config) {
    List<String> properties = new ArrayList<>();
    if (isOpaAuthorizationType(config, DEFAULT_AUTHORIZATION_TYPE_PROPERTY)) {
      properties.add(DEFAULT_AUTHORIZATION_TYPE_PROPERTY);
    }
    config
        .getPropertyNames()
        .forEach(property -> addOpaRealmTypeProperty(config, property, properties));
    return properties;
  }

  private static void addOpaRealmTypeProperty(
      Config config, String property, List<String> properties) {
    if (property.startsWith(AUTHORIZATION_REALMS_PREFIX)
        && property.endsWith(AUTHORIZATION_TYPE_SUFFIX)
        && isOpaAuthorizationType(config, property)) {
      properties.add(property);
    }
  }

  private static boolean isOpaAuthorizationType(Config config, String property) {
    return config
        .getOptionalValue(property, String.class)
        .filter(OPA_AUTHORIZATION_TYPE::equals)
        .isPresent();
  }
}
