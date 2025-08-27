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

import org.apache.polaris.core.admin.model.CreatePrincipalRoleRequest;
import org.apache.polaris.core.admin.model.PrincipalRole;
import org.apache.polaris.core.admin.model.UpdatePrincipalRoleRequest;

public class PrincipalRolesServiceEvents {
    public record AfterPrincipalRoleCreateEvent(PrincipalRole principalRole) implements PolarisEvent {}
    public record BeforePrincipalRoleCreateEvent(CreatePrincipalRoleRequest createPrincipalRoleRequest)
            implements PolarisEvent {}

    public record AfterPrincipalRoleDeleteEvent(String principalRoleName) implements PolarisEvent {}
    public record BeforePrincipalRoleDeleteEvent(String principalRoleName) implements PolarisEvent {}

    public record AfterPrincipalRoleGetEvent(PrincipalRole principalRole) implements PolarisEvent {}
    public record BeforePrincipalRoleGetEvent(String principalRoleName) implements PolarisEvent {}

    public record AfterPrincipalRoleUpdateEvent(PrincipalRole updatedPrincipalRole)
            implements PolarisEvent {}
    public record BeforePrincipalRoleUpdateEvent(
            String principalRoleName, UpdatePrincipalRoleRequest updateRequest) implements PolarisEvent {}

    public record AfterPrincipalRolesListEvent() implements PolarisEvent {}
    public record BeforePrincipalRolesListEvent() implements PolarisEvent {}

    public record AfterCatalogRoleAssignToPrincipalRoleEvent(
            String principalRoleName, String catalogName, String catalogRoleName) implements PolarisEvent {}
    public record BeforeCatalogRoleAssignToPrincipalRoleEvent(
            String principalRoleName, String catalogName, String catalogRoleName)
            implements PolarisEvent {}

    public record AfterCatalogRoleRevokeFromPrincipalRoleEvent(
            String principalRoleName, String catalogName, String catalogRoleName) implements PolarisEvent {}
    public record BeforeCatalogRoleRevokeFromPrincipalRoleEvent(
            String principalRoleName, String catalogName, String catalogRoleName) implements PolarisEvent {}

    public record AfterListAssigneePrincipalsForPrincipalRoleEvent(String principalRoleName) implements PolarisEvent {}
    public record BeforeListAssigneePrincipalsForPrincipalRoleEvent(String principalRoleName) implements PolarisEvent {}

    public record AfterListCatalogRolesForPrincipalRoleEvent(
            String principalRoleName, String catalogName) implements PolarisEvent {}
    public record BeforeListCatalogRolesForPrincipalRoleEvent(
            String principalRoleName, String catalogName) implements PolarisEvent {}
}
