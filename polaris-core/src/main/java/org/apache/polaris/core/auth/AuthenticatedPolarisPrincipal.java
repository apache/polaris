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
package org.apache.polaris.core.auth;

import jakarta.annotation.Nonnull;
import java.util.List;
import java.util.Set;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PrincipalRoleEntity;

/** Holds the results of request authentication. */
public class AuthenticatedPolarisPrincipal implements java.security.Principal {
  private final PolarisEntity principalEntity;
  private final Set<String> activatedPrincipalRoleNames;
  // only known and set after the above set of principal role names have been resolved. Before
  // this, this list is null
  private List<PrincipalRoleEntity> activatedPrincipalRoles;

  public AuthenticatedPolarisPrincipal(
      @Nonnull PolarisEntity principalEntity, @Nonnull Set<String> activatedPrincipalRoles) {
    this.principalEntity = principalEntity;
    this.activatedPrincipalRoleNames = activatedPrincipalRoles;
    this.activatedPrincipalRoles = null;
  }

  @Override
  public String getName() {
    return principalEntity.getName();
  }

  public PolarisEntity getPrincipalEntity() {
    return principalEntity;
  }

  public Set<String> getActivatedPrincipalRoleNames() {
    return activatedPrincipalRoleNames;
  }

  public List<PrincipalRoleEntity> getActivatedPrincipalRoles() {
    return activatedPrincipalRoles;
  }

  public void setActivatedPrincipalRoles(List<PrincipalRoleEntity> activatedPrincipalRoles) {
    this.activatedPrincipalRoles = activatedPrincipalRoles;
  }

  @Override
  public String toString() {
    return "principalEntity="
        + getPrincipalEntity()
        + ";activatedPrincipalRoleNames="
        + getActivatedPrincipalRoleNames()
        + ";activatedPrincipalRoles="
        + getActivatedPrincipalRoles();
  }
}
