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

import jakarta.ws.rs.core.SecurityContext;
import org.apache.polaris.core.context.CallContext;

/**
 * Represents an event listener that can respond to notable moments during Polaris's execution.
 * Event details are documented under the event objects themselves.
 */
public abstract class PolarisEventListener {
  /** {@link BeforeRequestRateLimitedEvent} */
  public void onBeforeRequestRateLimited(BeforeRequestRateLimitedEvent event) {}

  /** {@link BeforeTableCommitedEvent} */
  public void onBeforeTableCommited(
      BeforeTableCommitedEvent event, CallContext callCtx, SecurityContext securityContext) {}

  /** {@link AfterTableCommitedEvent} */
  public void onAfterTableCommited(
      AfterTableCommitedEvent event, CallContext callCtx, SecurityContext securityContext) {}

  /** {@link BeforeViewCommitedEvent} */
  public void onBeforeViewCommited(
      BeforeViewCommitedEvent event, CallContext callCtx, SecurityContext securityContext) {}

  /** {@link AfterViewCommitedEvent} */
  public void onAfterViewCommited(
      AfterViewCommitedEvent event, CallContext callCtx, SecurityContext securityContext) {}

  /** {@link BeforeTableRefreshedEvent} */
  public void onBeforeTableRefreshed(
      BeforeTableRefreshedEvent event, CallContext callCtx, SecurityContext securityContext) {}

  /** {@link AfterTableRefreshedEvent} */
  public void onAfterTableRefreshed(
      AfterTableRefreshedEvent event, CallContext callCtx, SecurityContext securityContext) {}

  /** {@link BeforeViewRefreshedEvent} */
  public void onBeforeViewRefreshed(
      BeforeViewRefreshedEvent event, CallContext callCtx, SecurityContext securityContext) {}

  /** {@link AfterViewRefreshedEvent} */
  public void onAfterViewRefreshed(
      AfterViewRefreshedEvent event, CallContext callCtx, SecurityContext securityContext) {}

  /** {@link BeforeTaskAttemptedEvent} */
  public void onBeforeTaskAttempted(BeforeTaskAttemptedEvent event) {}

  /** {@link AfterTaskAttemptedEvent} */
  public void onAfterTaskAttempted(AfterTaskAttemptedEvent event) {}

  /** {@link BeforeCatalogCreatedEvent} */
  public void onBeforeCatalogCreated(
      BeforeCatalogCreatedEvent event, CallContext callCtx, SecurityContext securityContext) {}

  /** {@link AfterCatalogCreatedEvent} */
  public void onAfterCatalogCreated(
      AfterCatalogCreatedEvent event, CallContext callCtx, SecurityContext securityContext) {}

  public void onBeforeCatalogDeleted(
      BeforeCatalogDeletedEvent event, CallContext callCtx, SecurityContext securityContext) {}

  public void onAfterCatalogDeleted(
      AfterCatalogDeletedEvent event, CallContext callCtx, SecurityContext securityContext) {}

  public void onBeforeCatalogGet(
      BeforeCatalogGetEvent event, CallContext callCtx, SecurityContext securityContext) {}

  public void onAfterCatalogGet(
      AfterCatalogGetEvent event, CallContext callCtx, SecurityContext securityContext) {}

  public void onBeforeCatalogUpdated(
      BeforeCatalogUpdatedEvent event, CallContext callCtx, SecurityContext securityContext) {}

  public void onAfterCatalogUpdated(
      AfterCatalogUpdatedEvent event, CallContext callCtx, SecurityContext securityContext) {}

  public void onBeforeCatalogList(
      BeforeCatalogListEvent event, CallContext callCtx, SecurityContext securityContext) {}

  public void onAfterCatalogList(
      AfterCatalogListEvent event, CallContext callCtx, SecurityContext securityContext) {}

  /** {@link BeforePrincipalCreateEvent} */
  public void onBeforePrincipalCreate(
      BeforePrincipalCreateEvent event, CallContext callCtx, SecurityContext securityContext) {}

  /** {@link AfterPrincipalCreateEvent} */
  public void onAfterPrincipalCreate(
      AfterPrincipalCreateEvent event, CallContext callCtx, SecurityContext securityContext) {}

  /** {@link BeforePrincipalDeleteEvent} */
  public void onBeforePrincipalDelete(
      BeforePrincipalDeleteEvent event, CallContext callCtx, SecurityContext securityContext) {}

  /** {@link AfterPrincipalDeleteEvent} */
  public void onAfterPrincipalDelete(
      AfterPrincipalDeleteEvent event, CallContext callCtx, SecurityContext securityContext) {}

  /** {@link BeforePrincipalGetEvent} */
  public void onBeforePrincipalGet(
      BeforePrincipalGetEvent event, CallContext callCtx, SecurityContext securityContext) {}

  /** {@link AfterPrincipalGetEvent} */
  public void onAfterPrincipalGet(
      AfterPrincipalGetEvent event, CallContext callCtx, SecurityContext securityContext) {}

  /** {@link BeforePrincipalUpdateEvent} */
  public void onBeforePrincipalUpdate(
      BeforePrincipalUpdateEvent event, CallContext callCtx, SecurityContext securityContext) {}

  /** {@link AfterPrincipalUpdateEvent} */
  public void onAfterPrincipalUpdate(
      AfterPrincipalUpdateEvent event, CallContext callCtx, SecurityContext securityContext) {}

  /** {@link BeforeCredentialsRotateEvent} */
  public void onBeforeCredentialsRotate(
      BeforeCredentialsRotateEvent event, CallContext callCtx, SecurityContext securityContext) {}

  /** {@link AfterCredentialsRotateEvent} */
  public void onAfterCredentialsRotate(
      AfterCredentialsRotateEvent event, CallContext callCtx, SecurityContext securityContext) {}

  /** {@link BeforePrincipalsListEvent} */
  public void onBeforePrincipalsList(
      BeforePrincipalsListEvent event, CallContext callCtx, SecurityContext securityContext) {}

  /** {@link AfterPrincipalsListEvent} */
  public void onAfterPrincipalsList(
      AfterPrincipalsListEvent event, CallContext callCtx, SecurityContext securityContext) {}

  /** {@link BeforePrincipalRoleCreateEvent} */
  public void onBeforePrincipalRoleCreate(
      BeforePrincipalRoleCreateEvent event, CallContext callCtx, SecurityContext securityContext) {}

  /** {@link AfterPrincipalRoleCreateEvent} */
  public void onAfterPrincipalRoleCreate(
      AfterPrincipalRoleCreateEvent event, CallContext callCtx, SecurityContext securityContext) {}

  /** {@link BeforePrincipalRoleDeleteEvent} */
  public void onBeforePrincipalRoleDelete(
      BeforePrincipalRoleDeleteEvent event, CallContext callCtx, SecurityContext securityContext) {}

  /** {@link AfterPrincipalRoleDeleteEvent} */
  public void onAfterPrincipalRoleDelete(
      AfterPrincipalRoleDeleteEvent event, CallContext callCtx, SecurityContext securityContext) {}

  /** {@link BeforePrincipalRoleGetEvent} */
  public void onBeforePrincipalRoleGet(
      BeforePrincipalRoleGetEvent event, CallContext callCtx, SecurityContext securityContext) {}

  /** {@link AfterPrincipalRoleGetEvent} */
  public void onAfterPrincipalRoleGet(
      AfterPrincipalRoleGetEvent event, CallContext callCtx, SecurityContext securityContext) {}

  /** {@link BeforePrincipalRoleUpdateEvent} */
  public void onBeforePrincipalRoleUpdate(
      BeforePrincipalRoleUpdateEvent event, CallContext callCtx, SecurityContext securityContext) {}

  /** {@link AfterPrincipalRoleUpdateEvent} */
  public void onAfterPrincipalRoleUpdate(
      AfterPrincipalRoleUpdateEvent event, CallContext callCtx, SecurityContext securityContext) {}

  /** {@link BeforePrincipalRolesListEvent} */
  public void onBeforePrincipalRolesList(
      BeforePrincipalRolesListEvent event, CallContext callCtx, SecurityContext securityContext) {}

  /** {@link AfterPrincipalRolesListEvent} */
  public void onAfterPrincipalRolesList(
      AfterPrincipalRolesListEvent event, CallContext callCtx, SecurityContext securityContext) {}

  /** {@link BeforeCatalogRoleCreateEvent} */
  public void onBeforeCatalogRoleCreate(
      BeforeCatalogRoleCreateEvent event, CallContext callCtx, SecurityContext securityContext) {}

  /** {@link AfterCatalogRoleCreateEvent} */
  public void onAfterCatalogRoleCreate(
      AfterCatalogRoleCreateEvent event, CallContext callCtx, SecurityContext securityContext) {}

  /** {@link BeforeCatalogRoleDeleteEvent} */
  public void onBeforeCatalogRoleDelete(
      BeforeCatalogRoleDeleteEvent event, CallContext callCtx, SecurityContext securityContext) {}

  /** {@link AfterCatalogRoleDeleteEvent} */
  public void onAfterCatalogRoleDelete(
      AfterCatalogRoleDeleteEvent event, CallContext callCtx, SecurityContext securityContext) {}

  /** {@link BeforeCatalogRoleGetEvent} */
  public void onBeforeCatalogRoleGet(
      BeforeCatalogRoleGetEvent event, CallContext callCtx, SecurityContext securityContext) {}

  /** {@link AfterCatalogRoleGetEvent} */
  public void onAfterCatalogRoleGet(
      AfterCatalogRoleGetEvent event, CallContext callCtx, SecurityContext securityContext) {}

  /** {@link BeforeCatalogRoleUpdateEvent} */
  public void onBeforeCatalogRoleUpdate(
      BeforeCatalogRoleUpdateEvent event, CallContext callCtx, SecurityContext securityContext) {}

  /** {@link AfterCatalogRoleUpdateEvent} */
  public void onAfterCatalogRoleUpdate(
      AfterCatalogRoleUpdateEvent event, CallContext callCtx, SecurityContext securityContext) {}

  /** {@link BeforeCatalogRolesListEvent} */
  public void onBeforeCatalogRolesList(
      BeforeCatalogRolesListEvent event, CallContext callCtx, SecurityContext securityContext) {}

  /** {@link AfterCatalogRolesListEvent} */
  public void onAfterCatalogRolesList(
      AfterCatalogRolesListEvent event, CallContext callCtx, SecurityContext securityContext) {}

  /** {@link BeforeAssignPrincipalRoleEvent} */
  public void onBeforeAssignPrincipalRole(
      BeforeAssignPrincipalRoleEvent event, CallContext callCtx, SecurityContext securityContext) {}

  /** {@link AfterAssignPrincipalRoleEvent} */
  public void onAfterAssignPrincipalRole(
      AfterAssignPrincipalRoleEvent event, CallContext callCtx, SecurityContext securityContext) {}

  /** {@link BeforeRevokePrincipalRoleEvent} */
  public void onBeforeRevokePrincipalRole(
      BeforeRevokePrincipalRoleEvent event, CallContext callCtx, SecurityContext securityContext) {}

  /** {@link AfterRevokePrincipalRoleEvent} */
  public void onAfterRevokePrincipalRole(
      AfterRevokePrincipalRoleEvent event, CallContext callCtx, SecurityContext securityContext) {}

  /** {@link BeforePrincipalRolesAssignedListEvent} */
  public void onBeforePrincipalRolesAssignedList(
      BeforePrincipalRolesAssignedListEvent event,
      CallContext callCtx,
      SecurityContext securityContext) {}

  /** {@link AfterPrincipalRolesAssignedListEvent} */
  public void onAfterPrincipalRolesAssignedList(
      AfterPrincipalRolesAssignedListEvent event,
      CallContext callCtx,
      SecurityContext securityContext) {}

  /** {@link BeforeCatalogRoleAssignToPrincipalRoleEvent} */
  public void onBeforeCatalogRoleAssignToPrincipalRole(
      BeforeCatalogRoleAssignToPrincipalRoleEvent event,
      CallContext callCtx,
      SecurityContext securityContext) {}

  /** {@link AfterCatalogRoleAssignToPrincipalRoleEvent} */
  public void onAfterCatalogRoleAssignToPrincipalRole(
      AfterCatalogRoleAssignToPrincipalRoleEvent event,
      CallContext callCtx,
      SecurityContext securityContext) {}

  /** {@link BeforeCatalogRoleRevokeFromPrincipalRoleEvent} */
  public void onBeforeCatalogRoleRevokeFromPrincipalRole(
      BeforeCatalogRoleRevokeFromPrincipalRoleEvent event,
      CallContext callCtx,
      SecurityContext securityContext) {}

  /** {@link AfterCatalogRoleRevokeFromPrincipalRoleEvent} */
  public void onAfterCatalogRoleRevokeFromPrincipalRole(
      AfterCatalogRoleRevokeFromPrincipalRoleEvent event,
      CallContext callCtx,
      SecurityContext securityContext) {}

  /** {@link BeforeListAssigneePrincipalsForPrincipalRoleEvent} */
  public void onBeforeListAssigneePrincipalsForPrincipalRole(
      BeforeListAssigneePrincipalsForPrincipalRoleEvent event,
      CallContext callCtx,
      SecurityContext securityContext) {}

  /** {@link AfterListAssigneePrincipalsForPrincipalRoleEvent} */
  public void onAfterListAssigneePrincipalsForPrincipalRole(
      AfterListAssigneePrincipalsForPrincipalRoleEvent event,
      CallContext callCtx,
      SecurityContext securityContext) {}

  /** {@link BeforeListCatalogRolesForPrincipalRoleEvent} */
  public void onBeforeListCatalogRolesForPrincipalRole(
      BeforeListCatalogRolesForPrincipalRoleEvent event,
      CallContext callCtx,
      SecurityContext securityContext) {}

  /** {@link AfterListCatalogRolesForPrincipalRoleEvent} */
  public void onAfterListCatalogRolesForPrincipalRole(
      AfterListCatalogRolesForPrincipalRoleEvent event,
      CallContext callCtx,
      SecurityContext securityContext) {}

  /** {@link BeforeAddGrantToCatalogRoleEvent} */
  public void onBeforeAddGrantToCatalogRole(
      BeforeAddGrantToCatalogRoleEvent event,
      CallContext callCtx,
      SecurityContext securityContext) {}

  /** {@link AfterAddGrantToCatalogRoleEvent} */
  public void onAfterAddGrantToCatalogRole(
      AfterAddGrantToCatalogRoleEvent event,
      CallContext callCtx,
      SecurityContext securityContext) {}

  /** {@link BeforeRevokeGrantFromCatalogRoleEvent} */
  public void onBeforeRevokeGrantFromCatalogRole(
      BeforeRevokeGrantFromCatalogRoleEvent event,
      CallContext callCtx,
      SecurityContext securityContext) {}

  /** {@link AfterRevokeGrantFromCatalogRoleEvent} */
  public void onAfterRevokeGrantFromCatalogRole(
      AfterRevokeGrantFromCatalogRoleEvent event,
      CallContext callCtx,
      SecurityContext securityContext) {}

  /** {@link BeforeListAssigneePrincipalRolesForCatalogRoleEvent} */
  public void onBeforeListAssigneePrincipalRolesForCatalogRole(
      BeforeListAssigneePrincipalRolesForCatalogRoleEvent event,
      CallContext callCtx,
      SecurityContext securityContext) {}

  /** {@link AfterListAssigneePrincipalRolesForCatalogRoleEvent} */
  public void onAfterListAssigneePrincipalRolesForCatalogRole(
      AfterListAssigneePrincipalRolesForCatalogRoleEvent event,
      CallContext callCtx,
      SecurityContext securityContext) {}

  /** {@link BeforeListGrantsForCatalogRoleEvent} */
  public void onBeforeListGrantsForCatalogRole(
      BeforeListGrantsForCatalogRoleEvent event,
      CallContext callCtx,
      SecurityContext securityContext) {}

  /** {@link AfterListGrantsForCatalogRoleEvent} */
  public void onAfterListGrantsForCatalogRole(
      AfterListGrantsForCatalogRoleEvent event,
      CallContext callCtx,
      SecurityContext securityContext) {}
}
