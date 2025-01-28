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
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.SecurityContext;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.apache.iceberg.exceptions.NotAuthorizedException;
import org.apache.polaris.core.auth.PolarisSecretsManager.PrincipalSecretsResult;
import org.apache.polaris.core.context.Realm;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.core.persistence.PolarisMetaStoreSession;
import org.apache.polaris.service.catalog.api.IcebergRestOAuth2ApiService;
import org.apache.polaris.service.types.TokenType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RequestScoped
@Identifier("test")
public class TestOAuth2ApiService implements IcebergRestOAuth2ApiService {
  private static final Logger LOGGER = LoggerFactory.getLogger(TestOAuth2ApiService.class);

  @Inject PolarisMetaStoreManager metaStoreManager;
  @Inject PolarisMetaStoreSession metaStoreSession;

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
      Realm realm,
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
            + realm.id()
            + ";role:"
            + scope.replaceAll(BasePolarisAuthenticator.PRINCIPAL_ROLE_PREFIX, ""));
    response.put("token_type", "bearer");
    response.put("expires_in", 3600);
    response.put("scope", Objects.requireNonNullElse(scope, "catalog"));
    return Response.ok(response).build();
  }

  private String getPrincipalName(String clientId) {
    PrincipalSecretsResult secretsResult =
        metaStoreManager.loadPrincipalSecrets(metaStoreSession, clientId);
    if (secretsResult.isSuccess()) {
      LOGGER.debug("Found principal secrets for client id {}", clientId);
      PolarisMetaStoreManager.EntityResult principalResult =
          metaStoreManager.loadEntity(
              metaStoreSession, 0L, secretsResult.getPrincipalSecrets().getPrincipalId());
      if (!principalResult.isSuccess()) {
        throw new NotAuthorizedException("Failed to load principal entity");
      }
      return principalResult.getEntity().getName();
    } else {
      LOGGER.debug(
          "Unable to find principal secrets for client id {} - trying as principal name", clientId);
      PolarisMetaStoreManager.EntityResult principalResult =
          metaStoreManager.readEntityByName(
              metaStoreSession,
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
}
