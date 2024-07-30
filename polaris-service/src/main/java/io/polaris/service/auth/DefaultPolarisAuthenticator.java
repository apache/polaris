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

import com.fasterxml.jackson.annotation.JsonProperty;
import io.polaris.core.auth.AuthenticatedPolarisPrincipal;
import io.polaris.core.context.CallContext;
import io.polaris.service.config.HasEntityManagerFactory;
import io.polaris.service.config.RealmEntityManagerFactory;
import java.util.Optional;

public class DefaultPolarisAuthenticator extends BasePolarisAuthenticator {
  private TokenBrokerFactory tokenBrokerFactory;

  @Override
  public Optional<AuthenticatedPolarisPrincipal> authenticate(String credentials) {
    TokenBroker handler =
        tokenBrokerFactory.apply(CallContext.getCurrentContext().getRealmContext());
    DecodedToken decodedToken = handler.verify(credentials);
    return getPrincipal(decodedToken);
  }

  @Override
  public void setEntityManagerFactory(RealmEntityManagerFactory entityManagerFactory) {
    super.setEntityManagerFactory(entityManagerFactory);
    if (tokenBrokerFactory instanceof HasEntityManagerFactory) {
      ((HasEntityManagerFactory) tokenBrokerFactory).setEntityManagerFactory(entityManagerFactory);
    }
  }

  @JsonProperty("tokenBroker")
  public void setTokenBroker(TokenBrokerFactory tokenBrokerFactory) {
    this.tokenBrokerFactory = tokenBrokerFactory;
  }
}
