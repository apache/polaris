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
package org.apache.polaris.service.test;

import static org.apache.polaris.service.context.DefaultContextResolver.REALM_PROPERTY_KEY;
import static org.assertj.core.api.Assertions.assertThat;

import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.apache.polaris.core.admin.model.GrantPrincipalRoleRequest;
import org.apache.polaris.core.admin.model.Principal;
import org.apache.polaris.core.admin.model.PrincipalRole;
import org.apache.polaris.core.admin.model.PrincipalWithCredentials;
import org.apache.polaris.core.entity.PolarisPrincipalSecrets;
import org.apache.polaris.service.auth.TokenUtils;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ExtensionContext.Namespace;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SnowmanCredentialsExtension
    implements BeforeAllCallback, AfterAllCallback, ParameterResolver {

  private static final Logger LOGGER = LoggerFactory.getLogger(SnowmanCredentialsExtension.class);
  private SnowmanCredentials snowmanCredentials;

  public record SnowmanIdentifier(String principalName, String principalRoleName) {}

  public record SnowmanCredentials(
      String clientId, String clientSecret, SnowmanIdentifier identifier) {}

  @Override
  public void beforeAll(ExtensionContext extensionContext) throws Exception {
    PolarisPrincipalSecrets adminSecrets = PolarisConnectionExtension.getAdminSecrets();
    String realm =
        extensionContext
            .getStore(Namespace.create(extensionContext.getRequiredTestClass()))
            .get(REALM_PROPERTY_KEY, String.class);

    if (adminSecrets == null) {
      LOGGER
          .atError()
          .log(
              "No admin secrets configured - you must also configure your test with PolarisConnectionExtension");
      return;
    }

    TestEnvironment testEnv = TestEnvironmentExtension.getEnv(extensionContext);
    String userToken =
        TokenUtils.getTokenFromSecrets(
            testEnv.apiClient(),
            testEnv.baseUri().toString(),
            adminSecrets.getPrincipalClientId(),
            adminSecrets.getMainSecret(),
            realm);

    SnowmanIdentifier snowmanIdentifier = getSnowmanIdentifier(testEnv);
    PrincipalRole principalRole = new PrincipalRole(snowmanIdentifier.principalRoleName());
    try (Response createPrResponse =
        testEnv
            .apiClient()
            .target(String.format("%s/api/management/v1/principal-roles", testEnv.baseUri()))
            .request("application/json")
            .header("Authorization", "Bearer " + userToken)
            .header(REALM_PROPERTY_KEY, realm)
            .post(Entity.json(principalRole))) {
      assertThat(createPrResponse)
          .returns(Response.Status.CREATED.getStatusCode(), Response::getStatus);
    }

    Principal principal = new Principal(snowmanIdentifier.principalName());

    try (Response createPResponse =
        testEnv
            .apiClient()
            .target(String.format("%s/api/management/v1/principals", testEnv.baseUri()))
            .request("application/json")
            .header("Authorization", "Bearer " + userToken) // how is token getting used?
            .header(REALM_PROPERTY_KEY, realm)
            .post(Entity.json(principal))) {
      assertThat(createPResponse)
          .returns(Response.Status.CREATED.getStatusCode(), Response::getStatus);
      PrincipalWithCredentials snowmanWithCredentials =
          createPResponse.readEntity(PrincipalWithCredentials.class);
      try (Response rotateResp =
          testEnv
              .apiClient()
              .target(
                  String.format(
                      "%s/api/management/v1/principals/%s/rotate",
                      testEnv.baseUri(), principal.getName()))
              .request(MediaType.APPLICATION_JSON)
              .header(
                  "Authorization",
                  "Bearer "
                      + TokenUtils.getTokenFromSecrets(
                          testEnv.apiClient(),
                          testEnv.baseUri().toString(),
                          snowmanWithCredentials.getCredentials().getClientId(),
                          snowmanWithCredentials.getCredentials().getClientSecret(),
                          realm))
              .header(REALM_PROPERTY_KEY, realm)
              .post(Entity.json(snowmanWithCredentials))) {

        assertThat(rotateResp).returns(Response.Status.OK.getStatusCode(), Response::getStatus);

        // Use the rotated credentials.
        snowmanWithCredentials = rotateResp.readEntity(PrincipalWithCredentials.class);
      }
      snowmanCredentials =
          new SnowmanCredentials(
              snowmanWithCredentials.getCredentials().getClientId(),
              snowmanWithCredentials.getCredentials().getClientSecret(),
              snowmanIdentifier);
    }
    try (Response assignPrResponse =
        testEnv
            .apiClient()
            .target(
                String.format(
                    "%s/api/management/v1/principals/%s/principal-roles",
                    testEnv.baseUri(), principal.getName()))
            .request("application/json")
            .header("Authorization", "Bearer " + userToken) // how is token getting used?
            .header(REALM_PROPERTY_KEY, realm)
            .put(Entity.json(new GrantPrincipalRoleRequest(principalRole)))) {
      assertThat(assignPrResponse)
          .returns(Response.Status.CREATED.getStatusCode(), Response::getStatus);
    }
  }

  @Override
  public void afterAll(ExtensionContext extensionContext) throws Exception {
    PolarisPrincipalSecrets adminSecrets = PolarisConnectionExtension.getAdminSecrets();
    String realm =
        extensionContext
            .getStore(Namespace.create(extensionContext.getRequiredTestClass()))
            .get(REALM_PROPERTY_KEY, String.class);

    if (adminSecrets == null) {
      LOGGER
          .atError()
          .log(
              "No admin secrets configured - you must also configure your test with PolarisConnectionExtension");
      return;
    }

    TestEnvironment testEnv = TestEnvironmentExtension.getEnv(extensionContext);
    String userToken =
        TokenUtils.getTokenFromSecrets(
            testEnv.apiClient(),
            testEnv.baseUri().toString(),
            adminSecrets.getPrincipalClientId(),
            adminSecrets.getMainSecret(),
            realm);

    SnowmanIdentifier snowmanIdentifier = getSnowmanIdentifier(testEnv);
    testEnv
        .apiClient()
        .target(
            String.format(
                "%s/api/management/v1/principal-roles/%s",
                testEnv.baseUri(), snowmanIdentifier.principalRoleName()))
        .request("application/json")
        .header("Authorization", "Bearer " + userToken)
        .header(REALM_PROPERTY_KEY, realm)
        .delete()
        .close();

    testEnv
        .apiClient()
        .target(
            String.format(
                "%s/api/management/v1/principals/%s",
                testEnv.baseUri(), snowmanIdentifier.principalName()))
        .request("application/json")
        .header("Authorization", "Bearer " + userToken)
        .header(REALM_PROPERTY_KEY, realm)
        .delete()
        .close();
  }

  // FIXME - this would be better done with a Credentials-specific annotation processor so
  // tests could declare which credentials they want (e.g., @TestCredentials("root") )
  // For now, snowman comes from here and root comes from PolarisConnectionExtension

  @Override
  public boolean supportsParameter(
      ParameterContext parameterContext, ExtensionContext extensionContext)
      throws ParameterResolutionException {

    return parameterContext.getParameter().getType() == SnowmanCredentials.class;
  }

  @Override
  public Object resolveParameter(
      ParameterContext parameterContext, ExtensionContext extensionContext)
      throws ParameterResolutionException {
    return snowmanCredentials;
  }

  private static SnowmanIdentifier getSnowmanIdentifier(TestEnvironment testEnv) {
    return new SnowmanIdentifier("snowman" + testEnv.testId(), "catalog-admin" + testEnv.testId());
  }
}
