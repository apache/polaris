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

import java.security.Principal;
import java.util.Optional;
import org.apache.iceberg.exceptions.NotAuthorizedException;
import org.apache.iceberg.exceptions.ServiceFailureException;

/**
 * An interface for authenticating principals based on provided credentials.
 *
 * @param <C> the type of credentials used for authentication
 * @param <P> the type of principal that is returned upon successful authentication
 */
public interface Authenticator<C, P extends Principal> {

  /**
   * Authenticates the given credentials and returns an optional principal.
   *
   * <p>If the credentials are not valid or if the authentication fails, implementations may choose
   * to return an empty optional or throw an exception. Returning empty will generally translate
   * into a {@link NotAuthorizedException}.
   *
   * @param credentials the credentials to authenticate
   * @return an optional principal if authentication is successful, or an empty optional if
   *     authentication fails.
   * @throws NotAuthorizedException if the credentials are not authorized
   * @throws ServiceFailureException if there is a failure in the authentication service
   */
  Optional<P> authenticate(C credentials) throws NotAuthorizedException, ServiceFailureException;
}
