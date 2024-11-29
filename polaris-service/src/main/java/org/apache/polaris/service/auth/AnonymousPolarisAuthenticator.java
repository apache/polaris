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
import java.util.Set;
import org.apache.polaris.core.auth.AuthenticatedPolarisPrincipal;
import org.apache.polaris.core.persistence.MetaStoreManagerFactory;

/**
 * An authenticator implementation that does not validate actor's identity, but always produce an
 * "anonymous" principal.
 */
@SuppressWarnings("unused")
public class AnonymousPolarisAuthenticator
    implements DiscoverableAuthenticator<String, AuthenticatedPolarisPrincipal> {

  private static final AuthenticatedPolarisPrincipal ANONYMOUS =
      new AuthenticatedPolarisPrincipalImpl(-1, "anonymous", Set.of());

  @Override
  public Optional<AuthenticatedPolarisPrincipal> authenticate(String s) {
    return Optional.of(ANONYMOUS);
  }

  @Override
  public void setMetaStoreManagerFactory(MetaStoreManagerFactory metaStoreManagerFactory) {
    // nop
  }
}
