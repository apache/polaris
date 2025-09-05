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
package org.apache.polaris.service.events.listeners;

import org.apache.polaris.service.events.AfterTableCommitedEvent;
import org.apache.polaris.service.events.AfterTableCreatedEvent;
import org.apache.polaris.service.events.AfterTableRefreshedEvent;
import org.apache.polaris.service.events.AfterTaskAttemptedEvent;
import org.apache.polaris.service.events.AfterViewCommitedEvent;
import org.apache.polaris.service.events.AfterViewRefreshedEvent;
import org.apache.polaris.service.events.BeforeRequestRateLimitedEvent;
import org.apache.polaris.service.events.BeforeTableCommitedEvent;
import org.apache.polaris.service.events.BeforeTableRefreshedEvent;
import org.apache.polaris.service.events.BeforeTaskAttemptedEvent;
import org.apache.polaris.service.events.BeforeViewCommitedEvent;
import org.apache.polaris.service.events.BeforeViewRefreshedEvent;
import org.apache.polaris.service.events.CatalogGenericTableServiceEvents;
import org.apache.polaris.service.events.CatalogsServiceEvents;
import org.apache.polaris.service.events.PrincipalRolesServiceEvents;
import org.apache.polaris.service.events.PrincipalsServiceEvents;

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

  /** {@link AfterTableCreatedEvent} */
  public void onAfterTableCreated(AfterTableCreatedEvent event) {}

  /** {@link CatalogsServiceEvents.BeforeCreateCatalogEvent} */
  public void onBeforeCreateCatalog(CatalogsServiceEvents.BeforeCreateCatalogEvent event) {}

  /** {@link CatalogsServiceEvents.AfterCreateCatalogEvent} */
  public void onAfterCreateCatalog(CatalogsServiceEvents.AfterCreateCatalogEvent event) {}

  /** {@link CatalogsServiceEvents.BeforeDeleteCatalogEvent} */
  public void onBeforeDeleteCatalog(CatalogsServiceEvents.BeforeDeleteCatalogEvent event) {}

  /** {@link CatalogsServiceEvents.AfterDeleteCatalogEvent} */
  public void onAfterDeleteCatalog(CatalogsServiceEvents.AfterDeleteCatalogEvent event) {}

  /** {@link CatalogsServiceEvents.BeforeGetCatalogEvent} */
  public void onBeforeGetCatalog(CatalogsServiceEvents.BeforeGetCatalogEvent event) {}

  /** {@link CatalogsServiceEvents.AfterGetCatalogEvent} */
  public void onAfterGetCatalog(CatalogsServiceEvents.AfterGetCatalogEvent event) {}

  /** {@link CatalogsServiceEvents.BeforeUpdateCatalogEvent} */
  public void onBeforeUpdateCatalog(CatalogsServiceEvents.BeforeUpdateCatalogEvent event) {}

  /** {@link CatalogsServiceEvents.AfterUpdateCatalogEvent} */
  public void onAfterUpdateCatalog(CatalogsServiceEvents.AfterUpdateCatalogEvent event) {}

  /** {@link CatalogsServiceEvents.BeforeListCatalogEvent} */
  public void onBeforeListCatalog(CatalogsServiceEvents.BeforeListCatalogEvent event) {}

  /** {@link CatalogsServiceEvents.AfterListCatalogEvent} */
  public void onAfterListCatalog(CatalogsServiceEvents.AfterListCatalogEvent event) {}

  /** {@link PrincipalsServiceEvents.BeforeCreatePrincipalEvent} */
  public void onBeforeCreatePrincipal(PrincipalsServiceEvents.BeforeCreatePrincipalEvent event) {}

  /** {@link PrincipalsServiceEvents.AfterCreatePrincipalEvent} */
  public void onAfterCreatePrincipal(PrincipalsServiceEvents.AfterCreatePrincipalEvent event) {}

  /** {@link PrincipalsServiceEvents.BeforeDeletePrincipalEvent} */
  public void onBeforeDeletePrincipal(PrincipalsServiceEvents.BeforeDeletePrincipalEvent event) {}

  /** {@link PrincipalsServiceEvents.AfterDeletePrincipalEvent} */
  public void onAfterDeletePrincipal(PrincipalsServiceEvents.AfterDeletePrincipalEvent event) {}

  /** {@link PrincipalsServiceEvents.BeforeGetPrincipalEvent} */
  public void onBeforeGetPrincipal(PrincipalsServiceEvents.BeforeGetPrincipalEvent event) {}

  /** {@link PrincipalsServiceEvents.AfterGetPrincipalEvent} */
  public void onAfterGetPrincipal(PrincipalsServiceEvents.AfterGetPrincipalEvent event) {}

  /** {@link PrincipalsServiceEvents.BeforeUpdatePrincipalEvent} */
  public void onBeforeUpdatePrincipal(PrincipalsServiceEvents.BeforeUpdatePrincipalEvent event) {}

  /** {@link PrincipalsServiceEvents.AfterUpdatePrincipalEvent} */
  public void onAfterUpdatePrincipal(PrincipalsServiceEvents.AfterUpdatePrincipalEvent event) {}

  /** {@link PrincipalsServiceEvents.BeforeRotateCredentialsEvent} */
  public void onBeforeRotateCredentials(
      PrincipalsServiceEvents.BeforeRotateCredentialsEvent event) {}

  /** {@link PrincipalsServiceEvents.AfterRotateCredentialsEvent} */
  public void onAfterRotateCredentials(PrincipalsServiceEvents.AfterRotateCredentialsEvent event) {}

  /** {@link PrincipalsServiceEvents.BeforeListPrincipalsEvent} */
  public void onBeforeListPrincipals(PrincipalsServiceEvents.BeforeListPrincipalsEvent event) {}

  /** {@link PrincipalsServiceEvents.AfterListPrincipalsEvent} */
  public void onAfterListPrincipals(PrincipalsServiceEvents.AfterListPrincipalsEvent event) {}

  /** {@link PrincipalsServiceEvents.BeforeResetCredentialsEvent} */
  public void onBeforeResetCredentials(PrincipalsServiceEvents.BeforeResetCredentialsEvent event) {}

  /** {@link PrincipalsServiceEvents.AfterResetCredentialsEvent} */
  public void onAfterResetCredentials(PrincipalsServiceEvents.AfterResetCredentialsEvent event) {}

  /** {@link PrincipalRolesServiceEvents.BeforeCreatePrincipalRoleEvent} */
  public void onBeforeCreatePrincipalRole(
      PrincipalRolesServiceEvents.BeforeCreatePrincipalRoleEvent event) {}

  /** {@link PrincipalRolesServiceEvents.AfterCreatePrincipalRoleEvent} */
  public void onAfterCreatePrincipalRole(
      PrincipalRolesServiceEvents.AfterCreatePrincipalRoleEvent event) {}

  /** {@link PrincipalRolesServiceEvents.BeforeDeletePrincipalRoleEvent} */
  public void onBeforeDeletePrincipalRole(
      PrincipalRolesServiceEvents.BeforeDeletePrincipalRoleEvent event) {}

  /** {@link PrincipalRolesServiceEvents.AfterDeletePrincipalRoleEvent} */
  public void onAfterDeletePrincipalRole(
      PrincipalRolesServiceEvents.AfterDeletePrincipalRoleEvent event) {}

  /** {@link PrincipalRolesServiceEvents.BeforeGetPrincipalRoleEvent} */
  public void onBeforeGetPrincipalRole(
      PrincipalRolesServiceEvents.BeforeGetPrincipalRoleEvent event) {}

  /** {@link PrincipalRolesServiceEvents.AfterGetPrincipalRoleEvent} */
  public void onAfterGetPrincipalRole(
      PrincipalRolesServiceEvents.AfterGetPrincipalRoleEvent event) {}

  /** {@link PrincipalRolesServiceEvents.BeforeUpdatePrincipalRoleEvent} */
  public void onBeforeUpdatePrincipalRole(
      PrincipalRolesServiceEvents.BeforeUpdatePrincipalRoleEvent event) {}

  /** {@link PrincipalRolesServiceEvents.AfterUpdatePrincipalRoleEvent} */
  public void onAfterUpdatePrincipalRole(
      PrincipalRolesServiceEvents.AfterUpdatePrincipalRoleEvent event) {}

  /** {@link PrincipalRolesServiceEvents.BeforeListPrincipalRolesEvent} */
  public void onBeforeListPrincipalRoles(
      PrincipalRolesServiceEvents.BeforeListPrincipalRolesEvent event) {}

  /** {@link PrincipalRolesServiceEvents.AfterListPrincipalRolesEvent} */
  public void onAfterListPrincipalRoles(
      PrincipalRolesServiceEvents.AfterListPrincipalRolesEvent event) {}

  /** {@link CatalogsServiceEvents.BeforeCreateCatalogRoleEvent} */
  public void onBeforeCreateCatalogRole(CatalogsServiceEvents.BeforeCreateCatalogRoleEvent event) {}

  /** {@link CatalogsServiceEvents.AfterCreateCatalogRoleEvent} */
  public void onAfterCreateCatalogRole(CatalogsServiceEvents.AfterCreateCatalogRoleEvent event) {}

  /** {@link CatalogsServiceEvents.BeforeDeleteCatalogRoleEvent} */
  public void onBeforeDeleteCatalogRole(CatalogsServiceEvents.BeforeDeleteCatalogRoleEvent event) {}

  /** {@link CatalogsServiceEvents.AfterDeleteCatalogRoleEvent} */
  public void onAfterDeleteCatalogRole(CatalogsServiceEvents.AfterDeleteCatalogRoleEvent event) {}

  /** {@link CatalogsServiceEvents.BeforeGetCatalogRoleEvent} */
  public void onBeforeGetCatalogRole(CatalogsServiceEvents.BeforeGetCatalogRoleEvent event) {}

  /** {@link CatalogsServiceEvents.AfterGetCatalogRoleEvent} */
  public void onAfterGetCatalogRole(CatalogsServiceEvents.AfterGetCatalogRoleEvent event) {}

  /** {@link CatalogsServiceEvents.BeforeUpdateCatalogRoleEvent} */
  public void onBeforeUpdateCatalogRole(CatalogsServiceEvents.BeforeUpdateCatalogRoleEvent event) {}

  /** {@link CatalogsServiceEvents.AfterUpdateCatalogRoleEvent} */
  public void onAfterUpdateCatalogRole(CatalogsServiceEvents.AfterUpdateCatalogRoleEvent event) {}

  /** {@link CatalogsServiceEvents.BeforeListCatalogRolesEvent} */
  public void onBeforeListCatalogRoles(CatalogsServiceEvents.BeforeListCatalogRolesEvent event) {}

  /** {@link CatalogsServiceEvents.AfterListCatalogRolesEvent} */
  public void onAfterListCatalogRoles(CatalogsServiceEvents.AfterListCatalogRolesEvent event) {}

  /** {@link PrincipalsServiceEvents.BeforeAssignPrincipalRoleEvent} */
  public void onBeforeAssignPrincipalRole(
      PrincipalsServiceEvents.BeforeAssignPrincipalRoleEvent event) {}

  /** {@link PrincipalsServiceEvents.AfterAssignPrincipalRoleEvent} */
  public void onAfterAssignPrincipalRole(
      PrincipalsServiceEvents.AfterAssignPrincipalRoleEvent event) {}

  /** {@link PrincipalsServiceEvents.BeforeRevokePrincipalRoleEvent} */
  public void onBeforeRevokePrincipalRole(
      PrincipalsServiceEvents.BeforeRevokePrincipalRoleEvent event) {}

  /** {@link PrincipalsServiceEvents.AfterRevokePrincipalRoleEvent} */
  public void onAfterRevokePrincipalRole(
      PrincipalsServiceEvents.AfterRevokePrincipalRoleEvent event) {}

  /** {@link PrincipalsServiceEvents.BeforeListAssignedPrincipalRolesEvent} */
  public void onBeforeListAssignedPrincipalRoles(
      PrincipalsServiceEvents.BeforeListAssignedPrincipalRolesEvent event) {}

  /** {@link PrincipalsServiceEvents.AfterListAssignedPrincipalRolesEvent} */
  public void onAfterListAssignedPrincipalRoles(
      PrincipalsServiceEvents.AfterListAssignedPrincipalRolesEvent event) {}

  /** {@link PrincipalRolesServiceEvents.BeforeAssignCatalogRoleToPrincipalRoleEvent} */
  public void onBeforeAssignCatalogRoleToPrincipalRole(
      PrincipalRolesServiceEvents.BeforeAssignCatalogRoleToPrincipalRoleEvent event) {}

  /** {@link PrincipalRolesServiceEvents.AfterAssignCatalogRoleToPrincipalRoleEvent} */
  public void onAfterAssignCatalogRoleToPrincipalRole(
      PrincipalRolesServiceEvents.AfterAssignCatalogRoleToPrincipalRoleEvent event) {}

  /** {@link PrincipalRolesServiceEvents.BeforeRevokeCatalogRoleFromPrincipalRoleEvent} */
  public void onBeforeRevokeCatalogRoleFromPrincipalRole(
      PrincipalRolesServiceEvents.BeforeRevokeCatalogRoleFromPrincipalRoleEvent event) {}

  /** {@link PrincipalRolesServiceEvents.AfterRevokeCatalogRoleFromPrincipalRoleEvent} */
  public void onAfterRevokeCatalogRoleFromPrincipalRole(
      PrincipalRolesServiceEvents.AfterRevokeCatalogRoleFromPrincipalRoleEvent event) {}

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
  public void onBeforeAddGrantToCatalogRole(
      CatalogsServiceEvents.BeforeAddGrantToCatalogRoleEvent event) {}

  /** {@link CatalogsServiceEvents.AfterAddGrantToCatalogRoleEvent} */
  public void onAfterAddGrantToCatalogRole(
      CatalogsServiceEvents.AfterAddGrantToCatalogRoleEvent event) {}

  /** {@link CatalogsServiceEvents.BeforeRevokeGrantFromCatalogRoleEvent} */
  public void onBeforeRevokeGrantFromCatalogRole(
      CatalogsServiceEvents.BeforeRevokeGrantFromCatalogRoleEvent event) {}

  /** {@link CatalogsServiceEvents.AfterRevokeGrantFromCatalogRoleEvent} */
  public void onAfterRevokeGrantFromCatalogRole(
      CatalogsServiceEvents.AfterRevokeGrantFromCatalogRoleEvent event) {}

  /** {@link CatalogsServiceEvents.BeforeListAssigneePrincipalRolesForCatalogRoleEvent} */
  public void onBeforeListAssigneePrincipalRolesForCatalogRole(
      CatalogsServiceEvents.BeforeListAssigneePrincipalRolesForCatalogRoleEvent event) {}

  /** {@link CatalogsServiceEvents.AfterListAssigneePrincipalRolesForCatalogRoleEvent} */
  public void onAfterListAssigneePrincipalRolesForCatalogRole(
      CatalogsServiceEvents.AfterListAssigneePrincipalRolesForCatalogRoleEvent event) {}

  /** {@link CatalogsServiceEvents.BeforeListGrantsForCatalogRoleEvent} */
  public void onBeforeListGrantsForCatalogRole(
      CatalogsServiceEvents.BeforeListGrantsForCatalogRoleEvent event) {}

  /** {@link CatalogsServiceEvents.AfterListGrantsForCatalogRoleEvent} */
  public void onAfterListGrantsForCatalogRole(
      CatalogsServiceEvents.AfterListGrantsForCatalogRoleEvent event) {}

  /** {@link CatalogGenericTableServiceEvents.BeforeCreateGenericTableEvent} */
  public void onBeforeCreateGenericTable(
      CatalogGenericTableServiceEvents.BeforeCreateGenericTableEvent event) {}

  /** {@link CatalogGenericTableServiceEvents.AfterCreateGenericTableEvent} */
  public void onAfterCreateGenericTable(
      CatalogGenericTableServiceEvents.AfterCreateGenericTableEvent event) {}

  /** {@link CatalogGenericTableServiceEvents.BeforeDropGenericTableEvent} */
  public void onBeforeDropGenericTable(
      CatalogGenericTableServiceEvents.BeforeDropGenericTableEvent event) {}

  /** {@link CatalogGenericTableServiceEvents.AfterDropGenericTableEvent} */
  public void onAfterDropGenericTable(
      CatalogGenericTableServiceEvents.AfterDropGenericTableEvent event) {}

  /** {@link CatalogGenericTableServiceEvents.BeforeListGenericTablesEvent} */
  public void onBeforeListGenericTables(
      CatalogGenericTableServiceEvents.BeforeListGenericTablesEvent event) {}

  /** {@link CatalogGenericTableServiceEvents.AfterListGenericTablesEvent} */
  public void onAfterListGenericTables(
      CatalogGenericTableServiceEvents.AfterListGenericTablesEvent event) {}

  /** {@link CatalogGenericTableServiceEvents.BeforeLoadGenericTableEvent} */
  public void onBeforeLoadGenericTable(
      CatalogGenericTableServiceEvents.BeforeLoadGenericTableEvent event) {}

  /** {@link CatalogGenericTableServiceEvents.AfterLoadGenericTableEvent} */
  public void onAfterLoadGenericTable(
      CatalogGenericTableServiceEvents.AfterLoadGenericTableEvent event) {}
}
