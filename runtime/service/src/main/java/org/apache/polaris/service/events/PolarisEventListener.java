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

  /** {@link CatalogsServiceEvents.BeforeCatalogCreatedEvent} */
  public void onBeforeCatalogCreated(CatalogsServiceEvents.BeforeCatalogCreatedEvent event) {}

  /** {@link CatalogsServiceEvents.AfterCatalogCreatedEvent} */
  public void onAfterCatalogCreated(CatalogsServiceEvents.AfterCatalogCreatedEvent event) {}

  /** {@link CatalogsServiceEvents.BeforeCatalogDeletedEvent} */
  public void onBeforeCatalogDeleted(CatalogsServiceEvents.BeforeCatalogDeletedEvent event) {}

  /** {@link CatalogsServiceEvents.AfterCatalogDeletedEvent} */
  public void onAfterCatalogDeleted(CatalogsServiceEvents.AfterCatalogDeletedEvent event) {}

  /** {@link CatalogsServiceEvents.BeforeCatalogGetEvent} */
  public void onBeforeCatalogGet(CatalogsServiceEvents.BeforeCatalogGetEvent event) {}

  /** {@link CatalogsServiceEvents.AfterCatalogGetEvent} */
  public void onAfterCatalogGet(CatalogsServiceEvents.AfterCatalogGetEvent event) {}

  /** {@link CatalogsServiceEvents.BeforeCatalogUpdatedEvent} */
  public void onBeforeCatalogUpdated(CatalogsServiceEvents.BeforeCatalogUpdatedEvent event) {}

  /** {@link CatalogsServiceEvents.AfterCatalogUpdatedEvent} */
  public void onAfterCatalogUpdated(CatalogsServiceEvents.AfterCatalogUpdatedEvent event) {}

  /** {@link CatalogsServiceEvents.BeforeCatalogListEvent} */
  public void onBeforeCatalogList(CatalogsServiceEvents.BeforeCatalogListEvent event) {}

  /** {@link CatalogsServiceEvents.AfterCatalogListEvent} */
  public void onAfterCatalogList(CatalogsServiceEvents.AfterCatalogListEvent event) {}

  /** {@link PrincipalsServiceEvents.BeforePrincipalCreateEvent} */
  public void onBeforePrincipalCreate(PrincipalsServiceEvents.BeforePrincipalCreateEvent event) {}

  /** {@link PrincipalsServiceEvents.AfterPrincipalCreateEvent} */
  public void onAfterPrincipalCreate(PrincipalsServiceEvents.AfterPrincipalCreateEvent event) {}

  /** {@link PrincipalsServiceEvents.BeforePrincipalDeleteEvent} */
  public void onBeforePrincipalDelete(PrincipalsServiceEvents.BeforePrincipalDeleteEvent event) {}

  /** {@link PrincipalsServiceEvents.AfterPrincipalDeleteEvent} */
  public void onAfterPrincipalDelete(PrincipalsServiceEvents.AfterPrincipalDeleteEvent event) {}

  /** {@link PrincipalsServiceEvents.BeforePrincipalGetEvent} */
  public void onBeforePrincipalGet(PrincipalsServiceEvents.BeforePrincipalGetEvent event) {}

  /** {@link PrincipalsServiceEvents.AfterPrincipalGetEvent} */
  public void onAfterPrincipalGet(PrincipalsServiceEvents.AfterPrincipalGetEvent event) {}

  /** {@link PrincipalsServiceEvents.BeforePrincipalUpdateEvent} */
  public void onBeforePrincipalUpdate(PrincipalsServiceEvents.BeforePrincipalUpdateEvent event) {}

  /** {@link PrincipalsServiceEvents.AfterPrincipalUpdateEvent} */
  public void onAfterPrincipalUpdate(PrincipalsServiceEvents.AfterPrincipalUpdateEvent event) {}

  /** {@link PrincipalsServiceEvents.BeforeCredentialsRotateEvent} */
  public void onBeforeCredentialsRotate(PrincipalsServiceEvents.BeforeCredentialsRotateEvent event) {}

  /** {@link PrincipalsServiceEvents.AfterCredentialsRotateEvent} */
  public void onAfterCredentialsRotate(PrincipalsServiceEvents.AfterCredentialsRotateEvent event) {}

  /** {@link PrincipalsServiceEvents.BeforePrincipalsListEvent} */
  public void onBeforePrincipalsList(PrincipalsServiceEvents.BeforePrincipalsListEvent event) {}

  /** {@link PrincipalsServiceEvents.AfterPrincipalsListEvent} */
  public void onAfterPrincipalsList(PrincipalsServiceEvents.AfterPrincipalsListEvent event) {}

  /** {@link PrincipalRolesServiceEvents.BeforePrincipalRoleCreateEvent} */
  public void onBeforePrincipalRoleCreate(PrincipalRolesServiceEvents.BeforePrincipalRoleCreateEvent event) {}

  /** {@link PrincipalRolesServiceEvents.AfterPrincipalRoleCreateEvent} */
  public void onAfterPrincipalRoleCreate(PrincipalRolesServiceEvents.AfterPrincipalRoleCreateEvent event) {}

  /** {@link PrincipalRolesServiceEvents.BeforePrincipalRoleDeleteEvent} */
  public void onBeforePrincipalRoleDelete(PrincipalRolesServiceEvents.BeforePrincipalRoleDeleteEvent event) {}

  /** {@link PrincipalRolesServiceEvents.AfterPrincipalRoleDeleteEvent} */
  public void onAfterPrincipalRoleDelete(PrincipalRolesServiceEvents.AfterPrincipalRoleDeleteEvent event) {}

  /** {@link PrincipalRolesServiceEvents.BeforePrincipalRoleGetEvent} */
  public void onBeforePrincipalRoleGet(PrincipalRolesServiceEvents.BeforePrincipalRoleGetEvent event) {}

  /** {@link PrincipalRolesServiceEvents.AfterPrincipalRoleGetEvent} */
  public void onAfterPrincipalRoleGet(PrincipalRolesServiceEvents.AfterPrincipalRoleGetEvent event) {}

  /** {@link PrincipalRolesServiceEvents.BeforePrincipalRoleUpdateEvent} */
  public void onBeforePrincipalRoleUpdate(PrincipalRolesServiceEvents.BeforePrincipalRoleUpdateEvent event) {}

  /** {@link PrincipalRolesServiceEvents.AfterPrincipalRoleUpdateEvent} */
  public void onAfterPrincipalRoleUpdate(PrincipalRolesServiceEvents.AfterPrincipalRoleUpdateEvent event) {}

  /** {@link PrincipalRolesServiceEvents.BeforePrincipalRolesListEvent} */
  public void onBeforePrincipalRolesList(PrincipalRolesServiceEvents.BeforePrincipalRolesListEvent event) {}

  /** {@link PrincipalRolesServiceEvents.AfterPrincipalRolesListEvent} */
  public void onAfterPrincipalRolesList(PrincipalRolesServiceEvents.AfterPrincipalRolesListEvent event) {}

  /** {@link CatalogsServiceEvents.BeforeCatalogRoleCreateEvent} */
  public void onBeforeCatalogRoleCreate(CatalogsServiceEvents.BeforeCatalogRoleCreateEvent event) {}

  /** {@link CatalogsServiceEvents.AfterCatalogRoleCreateEvent} */
  public void onAfterCatalogRoleCreate(CatalogsServiceEvents.AfterCatalogRoleCreateEvent event) {}

  /** {@link CatalogsServiceEvents.BeforeCatalogRoleDeleteEvent} */
  public void onBeforeCatalogRoleDelete(CatalogsServiceEvents.BeforeCatalogRoleDeleteEvent event) {}

  /** {@link CatalogsServiceEvents.AfterCatalogRoleDeleteEvent} */
  public void onAfterCatalogRoleDelete(CatalogsServiceEvents.AfterCatalogRoleDeleteEvent event) {}

  /** {@link CatalogsServiceEvents.BeforeCatalogRoleGetEvent} */
  public void onBeforeCatalogRoleGet(CatalogsServiceEvents.BeforeCatalogRoleGetEvent event) {}

  /** {@link CatalogsServiceEvents.AfterCatalogRoleGetEvent} */
  public void onAfterCatalogRoleGet(CatalogsServiceEvents.AfterCatalogRoleGetEvent event) {}

  /** {@link CatalogsServiceEvents.BeforeCatalogRoleUpdateEvent} */
  public void onBeforeCatalogRoleUpdate(CatalogsServiceEvents.BeforeCatalogRoleUpdateEvent event) {}

  /** {@link CatalogsServiceEvents.AfterCatalogRoleUpdateEvent} */
  public void onAfterCatalogRoleUpdate(CatalogsServiceEvents.AfterCatalogRoleUpdateEvent event) {}

  /** {@link CatalogsServiceEvents.BeforeCatalogRolesListEvent} */
  public void onBeforeCatalogRolesList(CatalogsServiceEvents.BeforeCatalogRolesListEvent event) {}

  /** {@link CatalogsServiceEvents.AfterCatalogRolesListEvent} */
  public void onAfterCatalogRolesList(CatalogsServiceEvents.AfterCatalogRolesListEvent event) {}

  /** {@link PrincipalsServiceEvents.BeforeAssignPrincipalRoleEvent} */
  public void onBeforeAssignPrincipalRole(PrincipalsServiceEvents.BeforeAssignPrincipalRoleEvent event) {}

  /** {@link PrincipalsServiceEvents.AfterAssignPrincipalRoleEvent} */
  public void onAfterAssignPrincipalRole(PrincipalsServiceEvents.AfterAssignPrincipalRoleEvent event) {}

  /** {@link PrincipalsServiceEvents.BeforeRevokePrincipalRoleEvent} */
  public void onBeforeRevokePrincipalRole(PrincipalsServiceEvents.BeforeRevokePrincipalRoleEvent event) {}

  /** {@link PrincipalsServiceEvents.AfterRevokePrincipalRoleEvent} */
  public void onAfterRevokePrincipalRole(PrincipalsServiceEvents.AfterRevokePrincipalRoleEvent event) {}

  /** {@link PrincipalsServiceEvents.BeforePrincipalRolesAssignedListEvent} */
  public void onBeforePrincipalRolesAssignedList(PrincipalsServiceEvents.BeforePrincipalRolesAssignedListEvent event) {}

  /** {@link PrincipalsServiceEvents.AfterPrincipalRolesAssignedListEvent} */
  public void onAfterPrincipalRolesAssignedList(PrincipalsServiceEvents.AfterPrincipalRolesAssignedListEvent event) {}

  /** {@link PrincipalRolesServiceEvents.BeforeCatalogRoleAssignToPrincipalRoleEvent} */
  public void onBeforeCatalogRoleAssignToPrincipalRole(
          PrincipalRolesServiceEvents.BeforeCatalogRoleAssignToPrincipalRoleEvent event) {}

  /** {@link PrincipalRolesServiceEvents.AfterCatalogRoleAssignToPrincipalRoleEvent} */
  public void onAfterCatalogRoleAssignToPrincipalRole(
          PrincipalRolesServiceEvents.AfterCatalogRoleAssignToPrincipalRoleEvent event) {}

  /** {@link PrincipalRolesServiceEvents.BeforeCatalogRoleRevokeFromPrincipalRoleEvent} */
  public void onBeforeCatalogRoleRevokeFromPrincipalRole(
          PrincipalRolesServiceEvents.BeforeCatalogRoleRevokeFromPrincipalRoleEvent event) {}

  /** {@link PrincipalRolesServiceEvents.AfterCatalogRoleRevokeFromPrincipalRoleEvent} */
  public void onAfterCatalogRoleRevokeFromPrincipalRole(
          PrincipalRolesServiceEvents.AfterCatalogRoleRevokeFromPrincipalRoleEvent event) {}

  /** {@link PrincipalRolesServiceEvents.BeforeListAssigneePrincipalsForPrincipalRoleEvent} */
  public void onBeforeListAssigneePrincipalsForPrincipalRole(
          PrincipalRolesServiceEvents.BeforeListAssigneePrincipalsForPrincipalRoleEvent event) {}

  /** {@link PrincipalRolesServiceEvents.AfterListAssigneePrincipalsForPrincipalRoleEvent} */
  public void onAfterListAssigneePrincipalsForPrincipalRole(
          PrincipalRolesServiceEvents.AfterListAssigneePrincipalsForPrincipalRoleEvent event) {}

  /** {@link PrincipalRolesServiceEvents.BeforeListCatalogRolesForPrincipalRoleEvent} */
  public void onBeforeListCatalogRolesForPrincipalRole(
          PrincipalRolesServiceEvents.BeforeListCatalogRolesForPrincipalRoleEvent event) {}

  /** {@link PrincipalRolesServiceEvents.AfterListCatalogRolesForPrincipalRoleEvent} */
  public void onAfterListCatalogRolesForPrincipalRole(
          PrincipalRolesServiceEvents.AfterListCatalogRolesForPrincipalRoleEvent event) {}

  /** {@link CatalogsServiceEvents.BeforeAddGrantToCatalogRoleEvent} */
  public void onBeforeAddGrantToCatalogRole(CatalogsServiceEvents.BeforeAddGrantToCatalogRoleEvent event) {}

  /** {@link CatalogsServiceEvents.AfterAddGrantToCatalogRoleEvent} */
  public void onAfterAddGrantToCatalogRole(CatalogsServiceEvents.AfterAddGrantToCatalogRoleEvent event) {}

  /** {@link CatalogsServiceEvents.BeforeRevokeGrantFromCatalogRoleEvent} */
  public void onBeforeRevokeGrantFromCatalogRole(CatalogsServiceEvents.BeforeRevokeGrantFromCatalogRoleEvent event) {}

  /** {@link CatalogsServiceEvents.AfterRevokeGrantFromCatalogRoleEvent} */
  public void onAfterRevokeGrantFromCatalogRole(CatalogsServiceEvents.AfterRevokeGrantFromCatalogRoleEvent event) {}

  /** {@link CatalogsServiceEvents.BeforeListAssigneePrincipalRolesForCatalogRoleEvent} */
  public void onBeforeListAssigneePrincipalRolesForCatalogRole(
          CatalogsServiceEvents.BeforeListAssigneePrincipalRolesForCatalogRoleEvent event) {}

  /** {@link CatalogsServiceEvents.AfterListAssigneePrincipalRolesForCatalogRoleEvent} */
  public void onAfterListAssigneePrincipalRolesForCatalogRole(
          CatalogsServiceEvents.AfterListAssigneePrincipalRolesForCatalogRoleEvent event) {}

  /** {@link CatalogsServiceEvents.BeforeListGrantsForCatalogRoleEvent} */
  public void onBeforeListGrantsForCatalogRole(CatalogsServiceEvents.BeforeListGrantsForCatalogRoleEvent event) {}

  /** {@link CatalogsServiceEvents.AfterListGrantsForCatalogRoleEvent} */
  public void onAfterListGrantsForCatalogRole(CatalogsServiceEvents.AfterListGrantsForCatalogRoleEvent event) {}
}
