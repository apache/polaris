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

import static org.apache.polaris.service.quarkus.auth.external.OidcTenantResolvingAugmentor.getOidcTenantConfig;

import io.quarkus.security.identity.SecurityIdentity;
import io.smallrye.common.annotation.Identifier;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A default implementation of {@link PrincipalRolesMapper}. It can optionally filter and map the
 * roles in the {@link SecurityIdentity} to Polaris roles, based on the configuration provided in
 * the OIDC tenant.
 */
@ApplicationScoped
@Identifier("default")
class DefaultPrincipalRolesMapper implements PrincipalRolesMapper {

  @Override
  public Set<String> mapPrincipalRoles(SecurityIdentity identity) {
    var rolesMapper = getOidcTenantConfig(identity).principalRolesMapper();
    return identity.getRoles().stream()
        .filter(rolesMapper.filterPredicate())
        .map(rolesMapper.mapperFunction())
        .collect(Collectors.toSet());
  }
}
