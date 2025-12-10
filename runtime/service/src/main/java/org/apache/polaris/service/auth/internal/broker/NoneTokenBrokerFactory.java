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
package org.apache.polaris.service.auth.internal.broker;

import io.smallrye.common.annotation.Identifier;
import jakarta.enterprise.context.ApplicationScoped;
import org.apache.iceberg.exceptions.NotAuthorizedException;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.service.auth.PolarisCredential;
import org.apache.polaris.service.auth.internal.service.OAuthError;
import org.apache.polaris.service.types.TokenType;

/** A no-op token broker factory used when authentication is delegated to an external IdP. */
@ApplicationScoped
@Identifier("none")
public class NoneTokenBrokerFactory implements TokenBrokerFactory {

  private static final TokenBroker DISABLED_TOKEN_BROKER =
      new TokenBroker() {
        @Override
        public boolean supportsGrantType(String grantType) {
          return false;
        }

        @Override
        public boolean supportsRequestedTokenType(TokenType tokenType) {
          return false;
        }

        @Override
        public TokenResponse generateFromClientSecrets(
            String clientId,
            String clientSecret,
            String grantType,
            String scope,
            TokenType requestedTokenType) {
          return TokenResponse.of(OAuthError.invalid_request);
        }

        @Override
        public PolarisCredential verify(String token) {
          throw new NotAuthorizedException("Token broker is disabled for external authentication");
        }

        @Override
        public TokenResponse generateFromToken(
            TokenType subjectTokenType,
            String subjectToken,
            String grantType,
            String scope,
            TokenType requestedTokenType) {
          return TokenResponse.of(OAuthError.invalid_request);
        }
      };

  @Override
  public TokenBroker create(
      PolarisMetaStoreManager polarisMetaStoreManager, PolarisCallContext polarisCallContext) {
    return DISABLED_TOKEN_BROKER;
  }
}
