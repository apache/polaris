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

import org.apache.polaris.core.admin.model.AddGrantRequest;
import org.apache.polaris.core.admin.model.Catalog;
import org.apache.polaris.core.admin.model.CatalogRole;
import org.apache.polaris.core.admin.model.GrantResource;
import org.apache.polaris.core.admin.model.RevokeGrantRequest;
import org.apache.polaris.core.admin.model.UpdateCatalogRequest;
import org.apache.polaris.core.admin.model.UpdateCatalogRoleRequest;
import org.apache.polaris.core.entity.PolarisPrivilege;

public class CatalogsServiceEvents {
    public record BeforeCatalogCreatedEvent(String catalogName) implements PolarisEvent {}
    public record AfterCatalogCreatedEvent(Catalog catalog) implements PolarisEvent {}

    public record BeforeCatalogDeletedEvent(String catalogName) implements PolarisEvent {}
    public record AfterCatalogDeletedEvent(String catalogName) implements PolarisEvent {}

    public record BeforeCatalogGetEvent(String catalogName) implements PolarisEvent {}
    public record AfterCatalogGetEvent(Catalog catalog) implements PolarisEvent {}

    public record BeforeCatalogUpdatedEvent(String catalogName, UpdateCatalogRequest updateRequest) implements PolarisEvent {}
    public record AfterCatalogUpdatedEvent(Catalog catalog) implements PolarisEvent {}

    public record BeforeCatalogListEvent() implements PolarisEvent {}
    public record AfterCatalogListEvent() implements PolarisEvent {}

    public record BeforeCatalogRoleCreateEvent(String catalogName, String catalogRoleName) implements PolarisEvent {}
    public record AfterCatalogRoleCreateEvent(String catalogName, CatalogRole catalogRole) implements PolarisEvent {}

    public record BeforeCatalogRoleDeleteEvent(String catalogName, String catalogRoleName) implements PolarisEvent {}
    public record AfterCatalogRoleDeleteEvent(String catalogName, String catalogRoleName) implements PolarisEvent {}

    public record BeforeCatalogRoleGetEvent(String catalogName, String catalogRoleName) implements PolarisEvent {}
    public record AfterCatalogRoleGetEvent(String catalogName, CatalogRole catalogRole) implements PolarisEvent {}

    public record BeforeCatalogRoleUpdateEvent(
            String catalogName, String catalogRoleName, UpdateCatalogRoleRequest updateRequest)
            implements PolarisEvent {}
    public record AfterCatalogRoleUpdateEvent(String catalogName, CatalogRole updatedCatalogRole) implements PolarisEvent {}

    public record BeforeCatalogRolesListEvent(String catalogName) implements PolarisEvent {}
    public record AfterCatalogRolesListEvent(String catalogName) implements PolarisEvent {}

    /**
     * Event fired before a grant is added to a catalog role in Polaris.
     *
     * @param catalogName the name of the catalog
     * @param catalogRoleName the name of the catalog role
     * @param grantRequest the grant request
     */
    public record BeforeAddGrantToCatalogRoleEvent(
            String catalogName, String catalogRoleName, AddGrantRequest grantRequest)
            implements PolarisEvent {}
    /**
     * Event fired after a grant is added to a catalog role in Polaris.
     *
     * @param catalogName the name of the catalog
     * @param catalogRoleName the name of the catalog role
     * @param privilege the privilege granted
     * @param grantResource the grant resource
     */
    public record AfterAddGrantToCatalogRoleEvent(
            String catalogName,
            String catalogRoleName,
            PolarisPrivilege privilege,
            GrantResource grantResource)
            implements PolarisEvent {}

    /**
     * Event fired before a grant is revoked from a catalog role in Polaris.
     *
     * @param catalogName the name of the catalog
     * @param catalogRoleName the name of the catalog role
     * @param grantRequest the revoke grant request
     * @param cascade whether the revoke is cascading
     */
    public record BeforeRevokeGrantFromCatalogRoleEvent(
            String catalogName, String catalogRoleName, RevokeGrantRequest grantRequest, Boolean cascade)
            implements PolarisEvent {}
    /**
     * Event fired after a grant is revoked from a catalog role in Polaris.
     *
     * @param catalogName the name of the catalog
     * @param catalogRoleName the name of the catalog role
     * @param privilege the privilege revoked
     * @param grantResource the revoke grant resource
     * @param cascade whether to cascade the revocation
     */
    public record AfterRevokeGrantFromCatalogRoleEvent(
            String catalogName,
            String catalogRoleName,
            PolarisPrivilege privilege,
            GrantResource grantResource,
            Boolean cascade)
            implements PolarisEvent {}

    public record BeforeListAssigneePrincipalRolesForCatalogRoleEvent(
            String catalogName, String catalogRoleName) implements PolarisEvent {}
    public record AfterListAssigneePrincipalRolesForCatalogRoleEvent(
            String catalogName, String catalogRoleName) implements PolarisEvent {}

    public record BeforeListGrantsForCatalogRoleEvent(String catalogName, String catalogRoleName)
            implements PolarisEvent {}
    public record AfterListGrantsForCatalogRoleEvent(String catalogName, String catalogRoleName)
            implements PolarisEvent {}
}
