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
package org.apache.polaris.service.quarkus.auth.external.mapping;

import io.quarkus.security.identity.SecurityIdentity;
import java.util.Optional;
import java.util.OptionalLong;

/**
 * Maps the {@link SecurityIdentity}, as provided by the OIDC authentication mechanism, to a Polaris
 * principal.
 *
 * <p>There are two ways to map the principal: by ID and by name. The ID is a long value that
 * uniquely identifies the principal in Polaris, while the name is a string that represents the
 * principal's unique name in Polaris.
 *
 * <p>At least one of these mappings must be provided, otherwise the authentication will fail. If
 * both mappings are available, the ID mapping takes precedence.
 */
public interface PrincipalMapper {

  /**
   * Maps the {@link SecurityIdentity} to a Polaris principal.
   *
   * @param identity the {@link SecurityIdentity} of the user
   * @return the Polaris principal, or an empty optional if no mapping is available
   */
  OptionalLong mapPrincipalId(SecurityIdentity identity);

  /**
   * Maps the {@link SecurityIdentity} to a Polaris principal name.
   *
   * @param identity the {@link SecurityIdentity} of the user
   * @return the Polaris principal name, or an empty optional if no mapping is available
   */
  Optional<String> mapPrincipalName(SecurityIdentity identity);
}
