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
import java.util.Optional;
import org.apache.polaris.core.auth.AuthenticatedPolarisPrincipal;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.persistence.MetaStoreManagerFactory;

@RequestScoped
@Identifier("default")
public class DefaultPolarisAuthenticator extends BasePolarisAuthenticator {

  private final TokenBrokerFactory tokenBrokerFactory;

  public DefaultPolarisAuthenticator() {
    this(null, null, null);
  }

  @Inject
  public DefaultPolarisAuthenticator(
      MetaStoreManagerFactory metaStoreManagerFactory,
      TokenBrokerFactory tokenBrokerFactory,
      CallContext callContext) {
    super(metaStoreManagerFactory, callContext);
    this.tokenBrokerFactory = tokenBrokerFactory;
  }

  @Override
  public Optional<AuthenticatedPolarisPrincipal> authenticate(String credentials) {
    TokenBroker handler = tokenBrokerFactory.apply(callContext.getRealmContext());
    DecodedToken decodedToken = handler.verify(credentials);
    return getPrincipal(decodedToken);
  }
}
