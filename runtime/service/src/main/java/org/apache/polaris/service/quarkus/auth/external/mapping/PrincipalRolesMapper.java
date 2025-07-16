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
import java.util.Set;
import org.apache.polaris.service.auth.DefaultAuthenticator;

/**
 * A mapper for extracting Polaris-specific role names from the {@link SecurityIdentity} of a user.
 */
public interface PrincipalRolesMapper {

  /**
   * Converts the role names in the identity to Polaris-specific role names.
   *
   * <p>The returned set must contain only valid role names; unrecognized role names should be
   * removed, and original names should be converted, if necessary.
   *
   * @implNote Polaris requires that all role names be prefixed with {@value
   *     DefaultAuthenticator#PRINCIPAL_ROLE_PREFIX}. The pseudo-role {@value
   *     DefaultAuthenticator#PRINCIPAL_ROLE_ALL} indicates that the user requests all roles that
   *     they have been granted. If this role is present, it should be the only role in the returned
   *     set.
   * @param identity the {@link SecurityIdentity} of the user
   * @return the converted role names
   */
  Set<String> mapPrincipalRoles(SecurityIdentity identity);
}
