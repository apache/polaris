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

import java.util.Optional;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.PrincipalEntity;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.service.types.TokenType;
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
      PolarisMetaStoreManager metaStoreManager, String clientId, String clientSecret) {
    // Validate the principal is present and secrets match
    PolarisCallContext polarisCallContext = CallContext.getCurrentContext().getPolarisCallContext();
    PolarisMetaStoreManager.PrincipalSecretsResult principalSecrets =
        metaStoreManager.loadPrincipalSecrets(polarisCallContext, clientId);
    if (!principalSecrets.isSuccess()) {
      return Optional.empty();
    }
    if (!principalSecrets.getPrincipalSecrets().matchesSecret(clientSecret)) {
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
