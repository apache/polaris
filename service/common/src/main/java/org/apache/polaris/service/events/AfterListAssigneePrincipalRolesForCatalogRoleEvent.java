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

package org.apache.polaris.service.events;

import org.apache.polaris.core.auth.AuthenticatedPolarisPrincipal;

/**
 * Event fired after the list of assignee principal roles for a catalog role is retrieved in
 * Polaris.
 */
public class AfterListAssigneePrincipalRolesForCatalogRoleEvent implements PolarisEvent {
  private final String catalogName;
  private final String catalogRoleName;
  private final String requestId;
  private final AuthenticatedPolarisPrincipal principal;

  /**
   * Constructs a new AfterListAssigneePrincipalRolesForCatalogRoleEvent.
   *
   * @param catalogName the name of the catalog
   * @param catalogRoleName the name of the catalog role
   * @param requestId the request ID for this operation
   * @param principal the authenticated principal performing the operation
   */
  public AfterListAssigneePrincipalRolesForCatalogRoleEvent(
      String catalogName,
      String catalogRoleName,
      String requestId,
      AuthenticatedPolarisPrincipal principal) {
    this.catalogName = catalogName;
    this.catalogRoleName = catalogRoleName;
    this.requestId = requestId;
    this.principal = principal;
  }

  public String getCatalogName() {
    return catalogName;
  }

  public String getCatalogRoleName() {
    return catalogRoleName;
  }

  public String getRequestId() {
    return requestId;
  }

  public AuthenticatedPolarisPrincipal getPrincipal() {
    return principal;
  }
}
