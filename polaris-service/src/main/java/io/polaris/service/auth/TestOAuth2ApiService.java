/*
 * Copyright (c) 2024 Snowflake Computing Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.polaris.service.auth;

import com.fasterxml.jackson.annotation.JsonTypeName;
import io.polaris.core.PolarisCallContext;
import io.polaris.core.context.CallContext;
import io.polaris.core.entity.PolarisEntitySubType;
import io.polaris.core.entity.PolarisEntityType;
import io.polaris.core.persistence.PolarisEntityManager;
import io.polaris.core.persistence.PolarisMetaStoreManager;
import io.polaris.service.config.HasEntityManagerFactory;
import io.polaris.service.config.OAuth2ApiService;
import io.polaris.service.config.RealmEntityManagerFactory;
import io.polaris.service.types.TokenType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.SecurityContext;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.apache.iceberg.exceptions.NotAuthorizedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@JsonTypeName("test")
public class TestOAuth2ApiService implements OAuth2ApiService, HasEntityManagerFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(TestOAuth2ApiService.class);

  private RealmEntityManagerFactory entityManagerFactory;

  @Override
  public Response getToken(
      String authHeader,
      String grantType,
      String scope,
      String clientId,
      String clientSecret,
      TokenType requestedTokenType,
      String subjectToken,
      TokenType subjectTokenType,
      String actorToken,
      TokenType actorTokenType,
      SecurityContext securityContext) {
    Map<String, Object> response = new HashMap<>();
    String principalName = getPrincipalName(clientId);
    response.put(
        "access_token",
        "principal:"
            + principalName
            + ";password:"
            + clientSecret
            + ";realm:"
            + CallContext.getCurrentContext().getRealmContext().getRealmIdentifier()
            + ";role:"
            + scope.replaceAll(BasePolarisAuthenticator.PRINCIPAL_ROLE_PREFIX, ""));
    response.put("token_type", "bearer");
    response.put("expires_in", 3600);
    response.put("scope", Objects.requireNonNullElse(scope, "catalog"));
    return Response.ok(response).build();
  }

  private String getPrincipalName(String clientId) {
    PolarisEntityManager entityManager =
        entityManagerFactory.getOrCreateEntityManager(
            CallContext.getCurrentContext().getRealmContext());
    PolarisCallContext polarisCallContext = CallContext.getCurrentContext().getPolarisCallContext();
    PolarisMetaStoreManager.PrincipalSecretsResult secretsResult =
        entityManager.getMetaStoreManager().loadPrincipalSecrets(polarisCallContext, clientId);
    if (secretsResult.isSuccess()) {
      LOGGER.debug("Found principal secrets for client id {}", clientId);
      PolarisMetaStoreManager.EntityResult principalResult =
          entityManager
              .getMetaStoreManager()
              .loadEntity(
                  polarisCallContext, 0L, secretsResult.getPrincipalSecrets().getPrincipalId());
      if (!principalResult.isSuccess()) {
        throw new NotAuthorizedException("Failed to load principal entity");
      }
      return principalResult.getEntity().getName();
    } else {
      LOGGER.debug(
          "Unable to find principal secrets for client id {} - trying as principal name", clientId);
      PolarisMetaStoreManager.EntityResult principalResult =
          entityManager
              .getMetaStoreManager()
              .readEntityByName(
                  polarisCallContext,
                  null,
                  PolarisEntityType.PRINCIPAL,
                  PolarisEntitySubType.NULL_SUBTYPE,
                  clientId);
      if (!principalResult.isSuccess()) {
        throw new NotAuthorizedException("Failed to read principal entity");
      }
      return principalResult.getEntity().getName();
    }
  }

  @Override
  public void setEntityManagerFactory(RealmEntityManagerFactory entityManagerFactory) {
    this.entityManagerFactory = entityManagerFactory;
  }

  @Override
  public void setTokenBroker(TokenBrokerFactory tokenBrokerFactory) {}
}
