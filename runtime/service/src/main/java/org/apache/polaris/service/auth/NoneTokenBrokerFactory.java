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
import jakarta.enterprise.context.ApplicationScoped;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.service.types.TokenType;

/** Default {@link TokenBrokerFactory} that produces token brokers that do not do anything. */
@ApplicationScoped
@Identifier("none")
public class NoneTokenBrokerFactory implements TokenBrokerFactory {

  public static final TokenBroker NONE_TOKEN_BROKER =
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
            PolarisCallContext polarisCallContext,
            TokenType requestedTokenType) {
          return null;
        }

        @Override
        public TokenResponse generateFromToken(
            TokenType subjectTokenType,
            String subjectToken,
            String grantType,
            String scope,
            PolarisCallContext polarisCallContext,
            TokenType requestedTokenType) {
          return null;
        }

        @Override
        public PolarisCredential verify(String token) {
          return null;
        }
      };

  @Override
  public TokenBroker apply(RealmContext realmContext) {
    return NONE_TOKEN_BROKER;
  }
}
