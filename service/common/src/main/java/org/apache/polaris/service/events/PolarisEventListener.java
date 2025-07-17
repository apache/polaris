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

/**
 * Represents an event listener that can respond to notable moments during Polaris's execution.
 * Event details are documented under the event objects themselves.
 */
public abstract class PolarisEventListener {
  /** {@link BeforeRequestRateLimitedEvent} */
  public void onBeforeRequestRateLimited(BeforeRequestRateLimitedEvent event) {}

  /** {@link BeforeTableCommitedEvent} */
  public void onBeforeTableCommited(BeforeTableCommitedEvent event) {}

  /** {@link AfterTableCommitedEvent} */
  public void onAfterTableCommited(AfterTableCommitedEvent event) {}

  /** {@link BeforeViewCommitedEvent} */
  public void onBeforeViewCommited(BeforeViewCommitedEvent event) {}

  /** {@link AfterViewCommitedEvent} */
  public void onAfterViewCommited(AfterViewCommitedEvent event) {}

  /** {@link BeforeTableRefreshedEvent} */
  public void onBeforeTableRefreshed(BeforeTableRefreshedEvent event) {}

  /** {@link AfterTableRefreshedEvent} */
  public void onAfterTableRefreshed(AfterTableRefreshedEvent event) {}

  /** {@link BeforeViewRefreshedEvent} */
  public void onBeforeViewRefreshed(BeforeViewRefreshedEvent event) {}

  /** {@link AfterViewRefreshedEvent} */
  public void onAfterViewRefreshed(AfterViewRefreshedEvent event) {}

  /** {@link BeforeTaskAttemptedEvent} */
  public void onBeforeTaskAttempted(BeforeTaskAttemptedEvent event) {}

  /** {@link AfterTaskAttemptedEvent} */
  public void onAfterTaskAttempted(AfterTaskAttemptedEvent event) {}

  /** {@link BeforeCatalogCreatedEvent} */
  public void onBeforeCatalogCreated(BeforeCatalogCreatedEvent event) {}

  /** {@link AfterCatalogCreatedEvent} */
  public void onAfterCatalogCreated(AfterCatalogCreatedEvent event) {}

  public void onBeforeCatalogDeleted(BeforeCatalogDeletedEvent event) {}

  public void onAfterCatalogDeleted(AfterCatalogDeletedEvent event) {}

  public void onBeforeCatalogGet(BeforeCatalogGetEvent event) {}

  public void onAfterCatalogGet(AfterCatalogGetEvent event) {}

  public void onBeforeCatalogUpdated(BeforeCatalogUpdatedEvent event) {}

  public void onAfterCatalogUpdated(AfterCatalogUpdatedEvent event) {}

  public void onBeforeCatalogList(BeforeCatalogListEvent event) {}

  public void onAfterCatalogList(AfterCatalogListEvent event) {}

  /** {@link BeforePrincipalCreateEvent} */
  public void onBeforePrincipalCreate(BeforePrincipalCreateEvent event) {}

  /** {@link AfterPrincipalCreateEvent} */
  public void onAfterPrincipalCreate(AfterPrincipalCreateEvent event) {}

  /** {@link BeforePrincipalDeleteEvent} */
  public void onBeforePrincipalDelete(BeforePrincipalDeleteEvent event) {}

  /** {@link AfterPrincipalDeleteEvent} */
  public void onAfterPrincipalDelete(AfterPrincipalDeleteEvent event) {}

  /** {@link BeforePrincipalGetEvent} */
  public void onBeforePrincipalGet(BeforePrincipalGetEvent event) {}

  /** {@link AfterPrincipalGetEvent} */
  public void onAfterPrincipalGet(AfterPrincipalGetEvent event) {}

  /** {@link BeforePrincipalUpdateEvent} */
  public void onBeforePrincipalUpdate(BeforePrincipalUpdateEvent event) {}

  /** {@link AfterPrincipalUpdateEvent} */
  public void onAfterPrincipalUpdate(AfterPrincipalUpdateEvent event) {}

  /** {@link BeforeCredentialsRotateEvent} */
  public void onBeforeCredentialsRotate(BeforeCredentialsRotateEvent event) {}

  /** {@link AfterCredentialsRotateEvent} */
  public void onAfterCredentialsRotate(AfterCredentialsRotateEvent event) {}

  /** {@link BeforePrincipalsListEvent} */
  public void onBeforePrincipalsList(BeforePrincipalsListEvent event) {}

  /** {@link AfterPrincipalsListEvent} */
  public void onAfterPrincipalsList(AfterPrincipalsListEvent event) {}

  /** {@link BeforePrincipalRoleCreateEvent} */
  public void onBeforePrincipalRoleCreate(BeforePrincipalRoleCreateEvent event) {}

  /** {@link AfterPrincipalRoleCreateEvent} */
  public void onAfterPrincipalRoleCreate(AfterPrincipalRoleCreateEvent event) {}

  /** {@link BeforePrincipalRoleDeleteEvent} */
  public void onBeforePrincipalRoleDelete(BeforePrincipalRoleDeleteEvent event) {}

  /** {@link AfterPrincipalRoleDeleteEvent} */
  public void onAfterPrincipalRoleDelete(AfterPrincipalRoleDeleteEvent event) {}

  /** {@link BeforePrincipalRoleGetEvent} */
  public void onBeforePrincipalRoleGet(BeforePrincipalRoleGetEvent event) {}

  /** {@link AfterPrincipalRoleGetEvent} */
  public void onAfterPrincipalRoleGet(AfterPrincipalRoleGetEvent event) {}

  /** {@link BeforePrincipalRoleUpdateEvent} */
  public void onBeforePrincipalRoleUpdate(BeforePrincipalRoleUpdateEvent event) {}

  /** {@link AfterPrincipalRoleUpdateEvent} */
  public void onAfterPrincipalRoleUpdate(AfterPrincipalRoleUpdateEvent event) {}

  /** {@link BeforePrincipalRolesListEvent} */
  public void onBeforePrincipalRolesList(BeforePrincipalRolesListEvent event) {}

  /** {@link AfterPrincipalRolesListEvent} */
  public void onAfterPrincipalRolesList(AfterPrincipalRolesListEvent event) {}

  /** {@link BeforeCatalogRoleCreateEvent} */
  public void onBeforeCatalogRoleCreate(BeforeCatalogRoleCreateEvent event) {}

  /** {@link AfterCatalogRoleCreateEvent} */
  public void onAfterCatalogRoleCreate(AfterCatalogRoleCreateEvent event) {}

  /** {@link BeforeCatalogRoleDeleteEvent} */
  public void onBeforeCatalogRoleDelete(BeforeCatalogRoleDeleteEvent event) {}

  /** {@link AfterCatalogRoleDeleteEvent} */
  public void onAfterCatalogRoleDelete(AfterCatalogRoleDeleteEvent event) {}

  /** {@link BeforeCatalogRoleGetEvent} */
  public void onBeforeCatalogRoleGet(BeforeCatalogRoleGetEvent event) {}

  /** {@link AfterCatalogRoleGetEvent} */
  public void onAfterCatalogRoleGet(AfterCatalogRoleGetEvent event) {}

  /** {@link BeforeCatalogRoleUpdateEvent} */
  public void onBeforeCatalogRoleUpdate(BeforeCatalogRoleUpdateEvent event) {}

  /** {@link AfterCatalogRoleUpdateEvent} */
  public void onAfterCatalogRoleUpdate(AfterCatalogRoleUpdateEvent event) {}

  /** {@link BeforeCatalogRolesListEvent} */
  public void onBeforeCatalogRolesList(BeforeCatalogRolesListEvent event) {}

  /** {@link AfterCatalogRolesListEvent} */
  public void onAfterCatalogRolesList(AfterCatalogRolesListEvent event) {}

  /** {@link BeforeAssignPrincipalRoleEvent} */
  public void onBeforeAssignPrincipalRole(BeforeAssignPrincipalRoleEvent event) {}

  /** {@link AfterAssignPrincipalRoleEvent} */
  public void onAfterAssignPrincipalRole(AfterAssignPrincipalRoleEvent event) {}

  /** {@link BeforeRevokePrincipalRoleEvent} */
  public void onBeforeRevokePrincipalRole(BeforeRevokePrincipalRoleEvent event) {}

  /** {@link AfterRevokePrincipalRoleEvent} */
  public void onAfterRevokePrincipalRole(AfterRevokePrincipalRoleEvent event) {}

  /** {@link BeforePrincipalRolesAssignedListEvent} */
  public void onBeforePrincipalRolesAssignedList(BeforePrincipalRolesAssignedListEvent event) {}

  /** {@link AfterPrincipalRolesAssignedListEvent} */
  public void onAfterPrincipalRolesAssignedList(AfterPrincipalRolesAssignedListEvent event) {}

  /** {@link BeforeCatalogRoleAssignToPrincipalRoleEvent} */
  public void onBeforeCatalogRoleAssignToPrincipalRole(
      BeforeCatalogRoleAssignToPrincipalRoleEvent event) {}

  /** {@link AfterCatalogRoleAssignToPrincipalRoleEvent} */
  public void onAfterCatalogRoleAssignToPrincipalRole(
      AfterCatalogRoleAssignToPrincipalRoleEvent event) {}

  /** {@link BeforeCatalogRoleRevokeFromPrincipalRoleEvent} */
  public void onBeforeCatalogRoleRevokeFromPrincipalRole(
      BeforeCatalogRoleRevokeFromPrincipalRoleEvent event) {}

  /** {@link AfterCatalogRoleRevokeFromPrincipalRoleEvent} */
  public void onAfterCatalogRoleRevokeFromPrincipalRole(
      AfterCatalogRoleRevokeFromPrincipalRoleEvent event) {}

  /** {@link BeforeListAssigneePrincipalsForPrincipalRoleEvent} */
  public void onBeforeListAssigneePrincipalsForPrincipalRole(
      BeforeListAssigneePrincipalsForPrincipalRoleEvent event) {}

  /** {@link AfterListAssigneePrincipalsForPrincipalRoleEvent} */
  public void onAfterListAssigneePrincipalsForPrincipalRole(
      AfterListAssigneePrincipalsForPrincipalRoleEvent event) {}

  /** {@link BeforeListCatalogRolesForPrincipalRoleEvent} */
  public void onBeforeListCatalogRolesForPrincipalRole(
      BeforeListCatalogRolesForPrincipalRoleEvent event) {}

  /** {@link AfterListCatalogRolesForPrincipalRoleEvent} */
  public void onAfterListCatalogRolesForPrincipalRole(
      AfterListCatalogRolesForPrincipalRoleEvent event) {}

  /** {@link BeforeAddGrantToCatalogRoleEvent} */
  public void onBeforeAddGrantToCatalogRole(BeforeAddGrantToCatalogRoleEvent event) {}

  /** {@link AfterAddGrantToCatalogRoleEvent} */
  public void onAfterAddGrantToCatalogRole(AfterAddGrantToCatalogRoleEvent event) {}

  /** {@link BeforeRevokeGrantFromCatalogRoleEvent} */
  public void onBeforeRevokeGrantFromCatalogRole(BeforeRevokeGrantFromCatalogRoleEvent event) {}

  /** {@link AfterRevokeGrantFromCatalogRoleEvent} */
  public void onAfterRevokeGrantFromCatalogRole(AfterRevokeGrantFromCatalogRoleEvent event) {}

  /** {@link BeforeListAssigneePrincipalRolesForCatalogRoleEvent} */
  public void onBeforeListAssigneePrincipalRolesForCatalogRole(
      BeforeListAssigneePrincipalRolesForCatalogRoleEvent event) {}

  /** {@link AfterListAssigneePrincipalRolesForCatalogRoleEvent} */
  public void onAfterListAssigneePrincipalRolesForCatalogRole(
      AfterListAssigneePrincipalRolesForCatalogRoleEvent event) {}

  /** {@link BeforeListGrantsForCatalogRoleEvent} */
  public void onBeforeListGrantsForCatalogRole(BeforeListGrantsForCatalogRoleEvent event) {}

  /** {@link AfterListGrantsForCatalogRoleEvent} */
  public void onAfterListGrantsForCatalogRole(AfterListGrantsForCatalogRoleEvent event) {}
}
