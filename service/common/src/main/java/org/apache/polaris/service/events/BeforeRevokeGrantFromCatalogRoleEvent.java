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

import org.apache.polaris.core.admin.model.RevokeGrantRequest;
import org.apache.polaris.core.auth.AuthenticatedPolarisPrincipal;

/** Event fired before a grant is revoked from a catalog role in Polaris. */
public class BeforeRevokeGrantFromCatalogRoleEvent implements PolarisEvent {
  private final String catalogName;
  private final String catalogRoleName;
  private final RevokeGrantRequest grantRequest;
  private final Boolean cascade;
  private final String requestId;
  private final AuthenticatedPolarisPrincipal principal;

  /**
   * Constructs a new BeforeRevokeGrantFromCatalogRoleEvent.
   *
   * @param catalogName the name of the catalog
   * @param catalogRoleName the name of the catalog role
   * @param grantRequest the revoke grant request
   * @param cascade whether the revoke is cascading
   * @param requestId the request ID for this operation
   * @param principal the authenticated principal performing the operation
   */
  public BeforeRevokeGrantFromCatalogRoleEvent(
      String catalogName,
      String catalogRoleName,
      RevokeGrantRequest grantRequest,
      Boolean cascade,
      String requestId,
      AuthenticatedPolarisPrincipal principal) {
    this.catalogName = catalogName;
    this.catalogRoleName = catalogRoleName;
    this.grantRequest = grantRequest;
    this.cascade = cascade;
    this.requestId = requestId;
    this.principal = principal;
  }

  public String getCatalogName() {
    return catalogName;
  }

  public String getCatalogRoleName() {
    return catalogRoleName;
  }

  public RevokeGrantRequest getGrantRequest() {
    return grantRequest;
  }

  public Boolean getCascade() {
    return cascade;
  }

  public String getRequestId() {
    return requestId;
  }

  public AuthenticatedPolarisPrincipal getPrincipal() {
    return principal;
  }
}
