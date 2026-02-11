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
package org.apache.polaris.extension.auth.opa.test;

import io.smallrye.common.annotation.Identifier;
import jakarta.enterprise.context.RequestScoped;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.iceberg.exceptions.NotAuthorizedException;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.service.auth.Authenticator;
import org.apache.polaris.service.auth.PolarisCredential;

@RequestScoped
@Identifier("passthrough")
public class PassthroughAuthenticator implements Authenticator {

  @Override
  public PolarisPrincipal authenticate(PolarisCredential credentials) {
    if (credentials == null) {
      throw new NotAuthorizedException("Missing credentials");
    }
    String principalName = credentials.getPrincipalName();
    if (principalName == null || principalName.isBlank()) {
      throw new NotAuthorizedException("Missing principal name");
    }
    Set<String> roles = credentials.getPrincipalRoles();
    return PolarisPrincipal.of(
        principalName, Map.of(), roles, Optional.ofNullable(credentials.getToken()));
  }
}
