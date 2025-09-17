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

import org.apache.polaris.service.events.CatalogGenericTableServiceEvents;
import org.apache.polaris.service.events.CatalogPolicyServiceEvents;
import org.apache.polaris.service.events.CatalogsServiceEvents;
import org.apache.polaris.service.events.PrincipalRolesServiceEvents;
import org.apache.polaris.service.events.PrincipalsServiceEvents;
import org.apache.polaris.service.events.AfterAttemptTaskEvent;
import org.apache.polaris.service.events.BeforeAttemptTaskEvent;
import org.apache.polaris.service.events.BeforeLimitRequestRateEvent;
import org.apache.polaris.service.events.IcebergRestCatalogEvents;

/**
 * Represents an event listener that can respond to notable moments during Polaris's execution.
 * Event details are documented under the event objects themselves.
 */
public abstract class PolarisEventListener {
  /** {@link BeforeLimitRequestRateEvent} */
  public void onBeforeLimitRequestRate(BeforeLimitRequestRateEvent event) {}

  /** {@link IcebergRestCatalogEvents.BeforeCommitTableEvent} */
  public void onBeforeCommitTable(IcebergRestCatalogEvents.BeforeCommitTableEvent event) {}

  /** {@link IcebergRestCatalogEvents.AfterCommitTableEvent} */
  public void onAfterCommitTable(IcebergRestCatalogEvents.AfterCommitTableEvent event) {}

  /** {@link IcebergRestCatalogEvents.BeforeCommitViewEvent} */
  public void onBeforeCommitView(IcebergRestCatalogEvents.BeforeCommitViewEvent event) {}

  /** {@link IcebergRestCatalogEvents.AfterCommitViewEvent} */
  public void onAfterCommitView(IcebergRestCatalogEvents.AfterCommitViewEvent event) {}

  /** {@link IcebergRestCatalogEvents.BeforeRefreshTableEvent} */
  public void onBeforeRefreshTable(IcebergRestCatalogEvents.BeforeRefreshTableEvent event) {}

  /** {@link IcebergRestCatalogEvents.AfterRefreshTableEvent} */
  public void onAfterRefreshTable(IcebergRestCatalogEvents.AfterRefreshTableEvent event) {}

  /** {@link IcebergRestCatalogEvents.BeforeRefreshViewEvent} */
  public void onBeforeRefreshView(IcebergRestCatalogEvents.BeforeRefreshViewEvent event) {}

  /** {@link IcebergRestCatalogEvents.AfterRefreshViewEvent} */
  public void onAfterRefreshView(IcebergRestCatalogEvents.AfterRefreshViewEvent event) {}

  /** {@link BeforeAttemptTaskEvent} */
  public void onBeforeAttemptTask(BeforeAttemptTaskEvent event) {}

  /** {@link AfterAttemptTaskEvent} */
  public void onAfterAttemptTask(AfterAttemptTaskEvent event) {}

  // Iceberg REST Catalog Namespace Events
  /** {@link IcebergRestCatalogEvents.BeforeCreateNamespaceEvent} */
  public void onBeforeCreateNamespace(IcebergRestCatalogEvents.BeforeCreateNamespaceEvent event) {}

  /** {@link IcebergRestCatalogEvents.AfterCreateNamespaceEvent} */
  public void onAfterCreateNamespace(IcebergRestCatalogEvents.AfterCreateNamespaceEvent event) {}

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

  /** {@link IcebergRestCatalogEvents.BeforeListNamespacesEvent} */
  public void onBeforeListNamespaces(IcebergRestCatalogEvents.BeforeListNamespacesEvent event) {}

  /** {@link IcebergRestCatalogEvents.AfterListNamespacesEvent} */
  public void onAfterListNamespaces(IcebergRestCatalogEvents.AfterListNamespacesEvent event) {}

  /** {@link IcebergRestCatalogEvents.BeforeLoadNamespaceMetadataEvent} */
  public void onBeforeLoadNamespaceMetadata(
      IcebergRestCatalogEvents.BeforeLoadNamespaceMetadataEvent event) {}

  /** {@link IcebergRestCatalogEvents.AfterLoadNamespaceMetadataEvent} */
  public void onAfterLoadNamespaceMetadata(
      IcebergRestCatalogEvents.AfterLoadNamespaceMetadataEvent event) {}

  /** {@link IcebergRestCatalogEvents.BeforeCheckExistsNamespaceEvent} */
  public void onBeforeCheckExistsNamespace(
      IcebergRestCatalogEvents.BeforeCheckExistsNamespaceEvent event) {}

  /** {@link IcebergRestCatalogEvents.AfterCheckExistsNamespaceEvent} */
  public void onAfterCheckExistsNamespace(
      IcebergRestCatalogEvents.AfterCheckExistsNamespaceEvent event) {}

  /** {@link IcebergRestCatalogEvents.BeforeDropNamespaceEvent} */
  public void onBeforeDropNamespace(IcebergRestCatalogEvents.BeforeDropNamespaceEvent event) {}

  /** {@link IcebergRestCatalogEvents.AfterDropNamespaceEvent} */
  public void onAfterDropNamespace(IcebergRestCatalogEvents.AfterDropNamespaceEvent event) {}

  /** {@link IcebergRestCatalogEvents.BeforeUpdateNamespacePropertiesEvent} */
  public void onBeforeUpdateNamespaceProperties(
      IcebergRestCatalogEvents.BeforeUpdateNamespacePropertiesEvent event) {}

  /** {@link IcebergRestCatalogEvents.AfterUpdateNamespacePropertiesEvent} */
  public void onAfterUpdateNamespaceProperties(
      IcebergRestCatalogEvents.AfterUpdateNamespacePropertiesEvent event) {}

  // Iceberg REST Catalog Table Events
  /** {@link IcebergRestCatalogEvents.BeforeCreateTableEvent} */
  public void onBeforeCreateTable(IcebergRestCatalogEvents.BeforeCreateTableEvent event) {}

  /** {@link IcebergRestCatalogEvents.AfterCreateTableEvent} */
  public void onAfterCreateTable(IcebergRestCatalogEvents.AfterCreateTableEvent event) {}

  /** {@link IcebergRestCatalogEvents.BeforeListTablesEvent} */
  public void onBeforeListTables(IcebergRestCatalogEvents.BeforeListTablesEvent event) {}

  /** {@link IcebergRestCatalogEvents.AfterListTablesEvent} */
  public void onAfterListTables(IcebergRestCatalogEvents.AfterListTablesEvent event) {}

  /** {@link IcebergRestCatalogEvents.BeforeLoadTableEvent} */
  public void onBeforeLoadTable(IcebergRestCatalogEvents.BeforeLoadTableEvent event) {}

  /** {@link IcebergRestCatalogEvents.AfterLoadTableEvent} */
  public void onAfterLoadTable(IcebergRestCatalogEvents.AfterLoadTableEvent event) {}

  /** {@link IcebergRestCatalogEvents.BeforeCheckExistsTableEvent} */
  public void onBeforeCheckExistsTable(
      IcebergRestCatalogEvents.BeforeCheckExistsTableEvent event) {}

  /** {@link IcebergRestCatalogEvents.AfterCheckExistsTableEvent} */
  public void onAfterCheckExistsTable(IcebergRestCatalogEvents.AfterCheckExistsTableEvent event) {}

  /** {@link IcebergRestCatalogEvents.BeforeDropTableEvent} */
  public void onBeforeDropTable(IcebergRestCatalogEvents.BeforeDropTableEvent event) {}

  /** {@link IcebergRestCatalogEvents.AfterDropTableEvent} */
  public void onAfterDropTable(IcebergRestCatalogEvents.AfterDropTableEvent event) {}

  /** {@link IcebergRestCatalogEvents.BeforeRegisterTableEvent} */
  public void onBeforeRegisterTable(IcebergRestCatalogEvents.BeforeRegisterTableEvent event) {}

  /** {@link IcebergRestCatalogEvents.AfterRegisterTableEvent} */
  public void onAfterRegisterTable(IcebergRestCatalogEvents.AfterRegisterTableEvent event) {}

  /** {@link IcebergRestCatalogEvents.BeforeRenameTableEvent} */
  public void onBeforeRenameTable(IcebergRestCatalogEvents.BeforeRenameTableEvent event) {}

  /** {@link IcebergRestCatalogEvents.AfterRenameTableEvent} */
  public void onAfterRenameTable(IcebergRestCatalogEvents.AfterRenameTableEvent event) {}

  /** {@link IcebergRestCatalogEvents.BeforeUpdateTableEvent} */
  public void onBeforeUpdateTable(IcebergRestCatalogEvents.BeforeUpdateTableEvent event) {}

  /** {@link IcebergRestCatalogEvents.AfterUpdateTableEvent} */
  public void onAfterUpdateTable(IcebergRestCatalogEvents.AfterUpdateTableEvent event) {}

  // Iceberg REST Catalog View Events
  /** {@link IcebergRestCatalogEvents.BeforeCreateViewEvent} */
  public void onBeforeCreateView(IcebergRestCatalogEvents.BeforeCreateViewEvent event) {}

  /** {@link IcebergRestCatalogEvents.AfterCreateViewEvent} */
  public void onAfterCreateView(IcebergRestCatalogEvents.AfterCreateViewEvent event) {}

  /** {@link IcebergRestCatalogEvents.BeforeListViewsEvent} */
  public void onBeforeListViews(IcebergRestCatalogEvents.BeforeListViewsEvent event) {}

  /** {@link IcebergRestCatalogEvents.AfterListViewsEvent} */
  public void onAfterListViews(IcebergRestCatalogEvents.AfterListViewsEvent event) {}

  /** {@link IcebergRestCatalogEvents.BeforeLoadViewEvent} */
  public void onBeforeLoadView(IcebergRestCatalogEvents.BeforeLoadViewEvent event) {}

  /** {@link IcebergRestCatalogEvents.AfterLoadViewEvent} */
  public void onAfterLoadView(IcebergRestCatalogEvents.AfterLoadViewEvent event) {}

  /** {@link IcebergRestCatalogEvents.BeforeCheckExistsViewEvent} */
  public void onBeforeCheckExistsView(IcebergRestCatalogEvents.BeforeCheckExistsViewEvent event) {}

  /** {@link IcebergRestCatalogEvents.AfterCheckExistsViewEvent} */
  public void onAfterCheckExistsView(IcebergRestCatalogEvents.AfterCheckExistsViewEvent event) {}

  /** {@link IcebergRestCatalogEvents.BeforeDropViewEvent} */
  public void onBeforeDropView(IcebergRestCatalogEvents.BeforeDropViewEvent event) {}

  /** {@link IcebergRestCatalogEvents.AfterDropViewEvent} */
  public void onAfterDropView(IcebergRestCatalogEvents.AfterDropViewEvent event) {}

  /** {@link IcebergRestCatalogEvents.BeforeRenameViewEvent} */
  public void onBeforeRenameView(IcebergRestCatalogEvents.BeforeRenameViewEvent event) {}

  /** {@link IcebergRestCatalogEvents.AfterRenameViewEvent} */
  public void onAfterRenameView(IcebergRestCatalogEvents.AfterRenameViewEvent event) {}

  /** {@link IcebergRestCatalogEvents.BeforeReplaceViewEvent} */
  public void onBeforeReplaceView(IcebergRestCatalogEvents.BeforeReplaceViewEvent event) {}

  /** {@link IcebergRestCatalogEvents.AfterReplaceViewEvent} */
  public void onAfterReplaceView(IcebergRestCatalogEvents.AfterReplaceViewEvent event) {}

  // Iceberg REST Catalog Credential Events
  /** {@link IcebergRestCatalogEvents.BeforeLoadCredentialsEvent} */
  public void onBeforeLoadCredentials(IcebergRestCatalogEvents.BeforeLoadCredentialsEvent event) {}

  /** {@link IcebergRestCatalogEvents.AfterLoadCredentialsEvent} */
  public void onAfterLoadCredentials(IcebergRestCatalogEvents.AfterLoadCredentialsEvent event) {}

  // Iceberg REST Catalog Transactions Events
  /** {@link IcebergRestCatalogEvents.BeforeCommitTransactionEvent} */
  public void onBeforeCommitTransaction(
      IcebergRestCatalogEvents.BeforeCommitTransactionEvent event) {}

  /** {@link IcebergRestCatalogEvents.AfterCommitTransactionEvent} */
  public void onAfterCommitTransaction(
      IcebergRestCatalogEvents.AfterCommitTransactionEvent event) {}

  // Iceberg REST Catalog Notification Events
  /** {@link IcebergRestCatalogEvents.BeforeSendNotificationEvent} */
  public void onBeforeSendNotification(
      IcebergRestCatalogEvents.BeforeSendNotificationEvent event) {}

  /** {@link IcebergRestCatalogEvents.AfterSendNotificationEvent} */
  public void onAfterSendNotification(IcebergRestCatalogEvents.AfterSendNotificationEvent event) {}

  // Iceberg REST Catalog Configuration Events
  /** {@link IcebergRestCatalogEvents.BeforeGetConfigEvent} */
  public void onBeforeGetConfig(IcebergRestCatalogEvents.BeforeGetConfigEvent event) {}

  /** {@link IcebergRestCatalogEvents.AfterGetConfigEvent} */
  public void onAfterGetConfig(IcebergRestCatalogEvents.AfterGetConfigEvent event) {}

  // Catalog Policy Service Events
  /** {@link CatalogPolicyServiceEvents.BeforeCreatePolicyEvent} */
  public void onBeforeCreatePolicy(CatalogPolicyServiceEvents.BeforeCreatePolicyEvent event) {}

  /** {@link CatalogPolicyServiceEvents.AfterCreatePolicyEvent} */
  public void onAfterCreatePolicy(CatalogPolicyServiceEvents.AfterCreatePolicyEvent event) {}

  /** {@link CatalogPolicyServiceEvents.BeforeListPoliciesEvent} */
  public void onBeforeListPolicies(CatalogPolicyServiceEvents.BeforeListPoliciesEvent event) {}

  /** {@link CatalogPolicyServiceEvents.AfterListPoliciesEvent} */
  public void onAfterListPolicies(CatalogPolicyServiceEvents.AfterListPoliciesEvent event) {}

  /** {@link CatalogPolicyServiceEvents.BeforeLoadPolicyEvent} */
  public void onBeforeLoadPolicy(CatalogPolicyServiceEvents.BeforeLoadPolicyEvent event) {}

  /** {@link CatalogPolicyServiceEvents.AfterLoadPolicyEvent} */
  public void onAfterLoadPolicy(CatalogPolicyServiceEvents.AfterLoadPolicyEvent event) {}

  /** {@link CatalogPolicyServiceEvents.BeforeUpdatePolicyEvent} */
  public void onBeforeUpdatePolicy(CatalogPolicyServiceEvents.BeforeUpdatePolicyEvent event) {}

  /** {@link CatalogPolicyServiceEvents.AfterUpdatePolicyEvent} */
  public void onAfterUpdatePolicy(CatalogPolicyServiceEvents.AfterUpdatePolicyEvent event) {}

  /** {@link CatalogPolicyServiceEvents.BeforeDropPolicyEvent} */
  public void onBeforeDropPolicy(CatalogPolicyServiceEvents.BeforeDropPolicyEvent event) {}

  /** {@link CatalogPolicyServiceEvents.AfterDropPolicyEvent} */
  public void onAfterDropPolicy(CatalogPolicyServiceEvents.AfterDropPolicyEvent event) {}

  /** {@link CatalogPolicyServiceEvents.BeforeAttachPolicyEvent} */
  public void onBeforeAttachPolicy(CatalogPolicyServiceEvents.BeforeAttachPolicyEvent event) {}

  /** {@link CatalogPolicyServiceEvents.AfterAttachPolicyEvent} */
  public void onAfterAttachPolicy(CatalogPolicyServiceEvents.AfterAttachPolicyEvent event) {}

  /** {@link CatalogPolicyServiceEvents.BeforeDetachPolicyEvent} */
  public void onBeforeDetachPolicy(CatalogPolicyServiceEvents.BeforeDetachPolicyEvent event) {}

  /** {@link CatalogPolicyServiceEvents.AfterDetachPolicyEvent} */
  public void onAfterDetachPolicy(CatalogPolicyServiceEvents.AfterDetachPolicyEvent event) {}

  /** {@link CatalogPolicyServiceEvents.BeforeGetApplicablePoliciesEvent} */
  public void onBeforeGetApplicablePolicies(
      CatalogPolicyServiceEvents.BeforeGetApplicablePoliciesEvent event) {}

  /** {@link CatalogPolicyServiceEvents.AfterGetApplicablePoliciesEvent} */
  public void onAfterGetApplicablePolicies(
      CatalogPolicyServiceEvents.AfterGetApplicablePoliciesEvent event) {}

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
