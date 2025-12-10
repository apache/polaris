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
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.iceberg.exceptions.NotAuthorizedException;
import org.apache.polaris.core.auth.PolarisPrincipal;

/**
 * Authenticator for principals supplied by an external identity provider.
 *
 * <p>Builds an in-memory {@link PolarisPrincipal} directly from credential claims without any
 * persistence lookups.
 */
@RequestScoped
@Identifier("external")
public class ExternalAuthenticator implements Authenticator {

  @Override
  public PolarisPrincipal authenticate(PolarisCredential credentials) {
    PolarisCredential externalCredentials = ensureExternal(credentials);

    String principalName = externalCredentials.getPrincipalName();
    Long principalId = externalCredentials.getPrincipalId();
    if (principalName == null && principalId != null) {
      principalName = String.valueOf(principalId);
    }
    if (principalName == null) {
      throw new NotAuthorizedException("Unable to authenticate external principal");
    }

    Set<String> principalRoles = externalCredentials.getPrincipalRoles();
    Map<String, String> properties = new HashMap<>();
    if (principalId != null) {
      properties.put("principalId", principalId.toString());
    }
    properties.put("external", Boolean.toString(true));
    if (externalCredentials.getPrincipalName() != null) {
      properties.put("principalName", externalCredentials.getPrincipalName());
    }

    return PolarisPrincipal.of(principalName, properties, principalRoles);
  }

  private PolarisCredential ensureExternal(PolarisCredential credentials) {
    Objects.requireNonNull(credentials, "credentials");
    if (credentials.isExternal()) {
      return credentials;
    }
    return PolarisCredential.of(
        credentials.getPrincipalId(),
        credentials.getPrincipalName(),
        credentials.getPrincipalRoles(),
        true);
  }
}
