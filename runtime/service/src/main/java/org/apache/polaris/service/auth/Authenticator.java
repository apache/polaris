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

import io.quarkus.security.AuthenticationFailedException;
import io.quarkus.security.identity.SecurityIdentity;
import org.apache.polaris.core.auth.PolarisPrincipal;

/**
 * An interface for authenticating {@linkplain PolarisPrincipal principals}.
 *
 * <p>Authenticators are used in both internal and external authentication scenarios.
 */
public interface Authenticator {

  /**
   * Authenticates the given {@link SecurityIdentity} and returns an authenticated {@link
   * PolarisPrincipal}.
   *
   * <p>If the credentials are not valid or if the authentication fails, implementations MUST throw
   * {@link AuthenticationFailedException}, which is mapped to 401 (Unauthorized) HTTP response
   * codes. It is also possible to throw other unchecked exceptions to signal unusual conditions;
   * these will be mapped to the appropriate HTTP response codes.
   *
   * @param identity the identity to authenticate
   * @return the authenticated principal
   * @throws AuthenticationFailedException if authentication fails
   */
  PolarisPrincipal authenticate(SecurityIdentity identity) throws AuthenticationFailedException;
}
