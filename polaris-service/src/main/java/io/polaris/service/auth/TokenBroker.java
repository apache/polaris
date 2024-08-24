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
package io.polaris.service.auth;

import io.polaris.core.PolarisCallContext;
import io.polaris.core.context.CallContext;
import io.polaris.core.entity.PolarisEntityType;
import io.polaris.core.entity.PrincipalEntity;
import io.polaris.core.persistence.PolarisEntityManager;
import io.polaris.core.persistence.PolarisMetaStoreManager;
import io.polaris.service.types.TokenType;
import java.util.Optional;
import org.jetbrains.annotations.NotNull;

/** Generic token class intended to be extended by different token types */
public interface TokenBroker {

  boolean supportsGrantType(String grantType);

  boolean supportsRequestedTokenType(TokenType tokenType);

  TokenResponse generateFromClientSecrets(
      final String clientId, final String clientSecret, final String grantType, final String scope);

  TokenResponse generateFromToken(
      TokenType tokenType, String subjectToken, final String grantType, final String scope);

  DecodedToken verify(String token);

  static @NotNull Optional<PrincipalEntity> findPrincipalEntity(
      PolarisEntityManager entityManager, String clientId, String clientSecret) {
    // Validate the principal is present and secrets match
    PolarisMetaStoreManager metaStoreManager = entityManager.getMetaStoreManager();
    PolarisCallContext polarisCallContext = CallContext.getCurrentContext().getPolarisCallContext();
    PolarisMetaStoreManager.PrincipalSecretsResult principalSecrets =
        metaStoreManager.loadPrincipalSecrets(polarisCallContext, clientId);
    if (!principalSecrets.isSuccess()) {
      return Optional.empty();
    }
    if (!principalSecrets.getPrincipalSecrets().getMainSecret().equals(clientSecret)
        && !principalSecrets.getPrincipalSecrets().getSecondarySecret().equals(clientSecret)) {
      return Optional.empty();
    }
    PolarisMetaStoreManager.EntityResult result =
        metaStoreManager.loadEntity(
            polarisCallContext, 0L, principalSecrets.getPrincipalSecrets().getPrincipalId());
    if (!result.isSuccess() || result.getEntity().getType() != PolarisEntityType.PRINCIPAL) {
      return Optional.empty();
    }
    return Optional.of(PrincipalEntity.of(result.getEntity()));
  }
}
