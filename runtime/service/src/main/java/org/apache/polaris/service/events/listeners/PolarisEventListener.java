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

import org.apache.polaris.service.events.AfterAttemptTaskEvent;
import org.apache.polaris.service.events.BeforeAttemptTaskEvent;
import org.apache.polaris.service.events.BeforeLimitRequestRateEvent;
import org.apache.polaris.service.events.CatalogGenericTableServiceEvents;
import org.apache.polaris.service.events.CatalogPolicyServiceEvents;
import org.apache.polaris.service.events.CatalogsServiceEvents;
import org.apache.polaris.service.events.IcebergRestCatalogEvents;
import org.apache.polaris.service.events.PrincipalRolesServiceEvents;
import org.apache.polaris.service.events.PrincipalsServiceEvents;

/**
 * Represents an event listener that can respond to notable moments during Polaris's execution.
 * Event details are documented under the event objects themselves.
 */
public interface PolarisEventListener {

  /*
   API Compatibility Note: any new methods added to this interface must have a default
   implementation in order to preserve binary compatibility.
  */

  // ============= Catalog Events =============

  default void onBeforeCreateCatalog(CatalogsServiceEvents.BeforeCreateCatalogEvent event) {}

  default void onAfterCreateCatalog(CatalogsServiceEvents.AfterCreateCatalogEvent event) {}

  default void onBeforeDeleteCatalog(CatalogsServiceEvents.BeforeDeleteCatalogEvent event) {}

  default void onAfterDeleteCatalog(CatalogsServiceEvents.AfterDeleteCatalogEvent event) {}

  default void onBeforeGetCatalog(CatalogsServiceEvents.BeforeGetCatalogEvent event) {}

  default void onAfterGetCatalog(CatalogsServiceEvents.AfterGetCatalogEvent event) {}

  default void onBeforeUpdateCatalog(CatalogsServiceEvents.BeforeUpdateCatalogEvent event) {}

  default void onAfterUpdateCatalog(CatalogsServiceEvents.AfterUpdateCatalogEvent event) {}

  default void onBeforeListCatalogs(CatalogsServiceEvents.BeforeListCatalogsEvent event) {}

  default void onAfterListCatalogs(CatalogsServiceEvents.AfterListCatalogsEvent event) {}

  // ============= Principal Events =============

  default void onBeforeCreatePrincipal(PrincipalsServiceEvents.BeforeCreatePrincipalEvent event) {}

  default void onAfterCreatePrincipal(PrincipalsServiceEvents.AfterCreatePrincipalEvent event) {}

  default void onBeforeDeletePrincipal(PrincipalsServiceEvents.BeforeDeletePrincipalEvent event) {}

  default void onAfterDeletePrincipal(PrincipalsServiceEvents.AfterDeletePrincipalEvent event) {}

  default void onBeforeGetPrincipal(PrincipalsServiceEvents.BeforeGetPrincipalEvent event) {}

  default void onAfterGetPrincipal(PrincipalsServiceEvents.AfterGetPrincipalEvent event) {}

  default void onBeforeUpdatePrincipal(PrincipalsServiceEvents.BeforeUpdatePrincipalEvent event) {}

  default void onAfterUpdatePrincipal(PrincipalsServiceEvents.AfterUpdatePrincipalEvent event) {}

  default void onBeforeRotateCredentials(
      PrincipalsServiceEvents.BeforeRotateCredentialsEvent event) {}

  default void onAfterRotateCredentials(
      PrincipalsServiceEvents.AfterRotateCredentialsEvent event) {}

  default void onBeforeListPrincipals(PrincipalsServiceEvents.BeforeListPrincipalsEvent event) {}

  default void onAfterListPrincipals(PrincipalsServiceEvents.AfterListPrincipalsEvent event) {}

  default void onBeforeResetCredentials(
      PrincipalsServiceEvents.BeforeResetCredentialsEvent event) {}

  default void onAfterResetCredentials(PrincipalsServiceEvents.AfterResetCredentialsEvent event) {}

  default void onBeforeAssignPrincipalRole(
      PrincipalsServiceEvents.BeforeAssignPrincipalRoleEvent event) {}

  default void onAfterAssignPrincipalRole(
      PrincipalsServiceEvents.AfterAssignPrincipalRoleEvent event) {}

  default void onBeforeRevokePrincipalRole(
      PrincipalsServiceEvents.BeforeRevokePrincipalRoleEvent event) {}

  default void onAfterRevokePrincipalRole(
      PrincipalsServiceEvents.AfterRevokePrincipalRoleEvent event) {}

  default void onBeforeListAssignedPrincipalRoles(
      PrincipalsServiceEvents.BeforeListAssignedPrincipalRolesEvent event) {}

  default void onAfterListAssignedPrincipalRoles(
      PrincipalsServiceEvents.AfterListAssignedPrincipalRolesEvent event) {}

  // ============= Principal Role Events =============

  default void onBeforeCreatePrincipalRole(
      PrincipalRolesServiceEvents.BeforeCreatePrincipalRoleEvent event) {}

  default void onAfterCreatePrincipalRole(
      PrincipalRolesServiceEvents.AfterCreatePrincipalRoleEvent event) {}

  default void onBeforeDeletePrincipalRole(
      PrincipalRolesServiceEvents.BeforeDeletePrincipalRoleEvent event) {}

  default void onAfterDeletePrincipalRole(
      PrincipalRolesServiceEvents.AfterDeletePrincipalRoleEvent event) {}

  default void onBeforeGetPrincipalRole(
      PrincipalRolesServiceEvents.BeforeGetPrincipalRoleEvent event) {}

  default void onAfterGetPrincipalRole(
      PrincipalRolesServiceEvents.AfterGetPrincipalRoleEvent event) {}

  default void onBeforeUpdatePrincipalRole(
      PrincipalRolesServiceEvents.BeforeUpdatePrincipalRoleEvent event) {}

  default void onAfterUpdatePrincipalRole(
      PrincipalRolesServiceEvents.AfterUpdatePrincipalRoleEvent event) {}

  default void onBeforeListPrincipalRoles(
      PrincipalRolesServiceEvents.BeforeListPrincipalRolesEvent event) {}

  default void onAfterListPrincipalRoles(
      PrincipalRolesServiceEvents.AfterListPrincipalRolesEvent event) {}

  // ============= Catalog Role Events =============

  default void onBeforeCreateCatalogRole(
      CatalogsServiceEvents.BeforeCreateCatalogRoleEvent event) {}

  default void onAfterCreateCatalogRole(CatalogsServiceEvents.AfterCreateCatalogRoleEvent event) {}

  default void onBeforeDeleteCatalogRole(
      CatalogsServiceEvents.BeforeDeleteCatalogRoleEvent event) {}

  default void onAfterDeleteCatalogRole(CatalogsServiceEvents.AfterDeleteCatalogRoleEvent event) {}

  default void onBeforeGetCatalogRole(CatalogsServiceEvents.BeforeGetCatalogRoleEvent event) {}

  default void onAfterGetCatalogRole(CatalogsServiceEvents.AfterGetCatalogRoleEvent event) {}

  default void onBeforeUpdateCatalogRole(
      CatalogsServiceEvents.BeforeUpdateCatalogRoleEvent event) {}

  default void onAfterUpdateCatalogRole(CatalogsServiceEvents.AfterUpdateCatalogRoleEvent event) {}

  default void onBeforeListCatalogRoles(CatalogsServiceEvents.BeforeListCatalogRolesEvent event) {}

  default void onAfterListCatalogRoles(CatalogsServiceEvents.AfterListCatalogRolesEvent event) {}

  default void onBeforeAssignCatalogRoleToPrincipalRole(
      PrincipalRolesServiceEvents.BeforeAssignCatalogRoleToPrincipalRoleEvent event) {}

  default void onAfterAssignCatalogRoleToPrincipalRole(
      PrincipalRolesServiceEvents.AfterAssignCatalogRoleToPrincipalRoleEvent event) {}

  default void onBeforeRevokeCatalogRoleFromPrincipalRole(
      PrincipalRolesServiceEvents.BeforeRevokeCatalogRoleFromPrincipalRoleEvent event) {}

  default void onAfterRevokeCatalogRoleFromPrincipalRole(
      PrincipalRolesServiceEvents.AfterRevokeCatalogRoleFromPrincipalRoleEvent event) {}

  default void onBeforeListAssigneePrincipalsForPrincipalRole(
      PrincipalRolesServiceEvents.BeforeListAssigneePrincipalsForPrincipalRoleEvent event) {}

  default void onAfterListAssigneePrincipalsForPrincipalRole(
      PrincipalRolesServiceEvents.AfterListAssigneePrincipalsForPrincipalRoleEvent event) {}

  default void onBeforeListCatalogRolesForPrincipalRole(
      PrincipalRolesServiceEvents.BeforeListCatalogRolesForPrincipalRoleEvent event) {}

  default void onAfterListCatalogRolesForPrincipalRole(
      PrincipalRolesServiceEvents.AfterListCatalogRolesForPrincipalRoleEvent event) {}

  default void onBeforeAddGrantToCatalogRole(
      CatalogsServiceEvents.BeforeAddGrantToCatalogRoleEvent event) {}

  default void onAfterAddGrantToCatalogRole(
      CatalogsServiceEvents.AfterAddGrantToCatalogRoleEvent event) {}

  default void onBeforeRevokeGrantFromCatalogRole(
      CatalogsServiceEvents.BeforeRevokeGrantFromCatalogRoleEvent event) {}

  default void onAfterRevokeGrantFromCatalogRole(
      CatalogsServiceEvents.AfterRevokeGrantFromCatalogRoleEvent event) {}

  default void onBeforeListAssigneePrincipalRolesForCatalogRole(
      CatalogsServiceEvents.BeforeListAssigneePrincipalRolesForCatalogRoleEvent event) {}

  default void onAfterListAssigneePrincipalRolesForCatalogRole(
      CatalogsServiceEvents.AfterListAssigneePrincipalRolesForCatalogRoleEvent event) {}

  default void onBeforeListGrantsForCatalogRole(
      CatalogsServiceEvents.BeforeListGrantsForCatalogRoleEvent event) {}

  default void onAfterListGrantsForCatalogRole(
      CatalogsServiceEvents.AfterListGrantsForCatalogRoleEvent event) {}

  // ============= Iceberg REST Namespace Events =============

  default void onBeforeCreateNamespace(IcebergRestCatalogEvents.BeforeCreateNamespaceEvent event) {}

  default void onAfterCreateNamespace(IcebergRestCatalogEvents.AfterCreateNamespaceEvent event) {}

  default void onBeforeListNamespaces(IcebergRestCatalogEvents.BeforeListNamespacesEvent event) {}

  default void onAfterListNamespaces(IcebergRestCatalogEvents.AfterListNamespacesEvent event) {}

  default void onBeforeLoadNamespaceMetadata(
      IcebergRestCatalogEvents.BeforeLoadNamespaceMetadataEvent event) {}

  default void onAfterLoadNamespaceMetadata(
      IcebergRestCatalogEvents.AfterLoadNamespaceMetadataEvent event) {}

  default void onBeforeCheckExistsNamespace(
      IcebergRestCatalogEvents.BeforeCheckExistsNamespaceEvent event) {}

  default void onAfterCheckExistsNamespace(
      IcebergRestCatalogEvents.AfterCheckExistsNamespaceEvent event) {}

  default void onBeforeDropNamespace(IcebergRestCatalogEvents.BeforeDropNamespaceEvent event) {}

  default void onAfterDropNamespace(IcebergRestCatalogEvents.AfterDropNamespaceEvent event) {}

  default void onBeforeUpdateNamespaceProperties(
      IcebergRestCatalogEvents.BeforeUpdateNamespacePropertiesEvent event) {}

  default void onAfterUpdateNamespaceProperties(
      IcebergRestCatalogEvents.AfterUpdateNamespacePropertiesEvent event) {}

  // ============= Iceberg REST Table Events =============

  default void onBeforeCreateTable(IcebergRestCatalogEvents.BeforeCreateTableEvent event) {}

  default void onAfterCreateTable(IcebergRestCatalogEvents.AfterCreateTableEvent event) {}

  default void onBeforeCommitTable(IcebergRestCatalogEvents.BeforeCommitTableEvent event) {}

  default void onStageCommitTable(IcebergRestCatalogEvents.StageCommitTableEvent event) {}

  default void onAfterCommitTable(IcebergRestCatalogEvents.AfterCommitTableEvent event) {}

  default void onBeforeRefreshTable(IcebergRestCatalogEvents.BeforeRefreshTableEvent event) {}

  default void onAfterRefreshTable(IcebergRestCatalogEvents.AfterRefreshTableEvent event) {}

  default void onBeforeListTables(IcebergRestCatalogEvents.BeforeListTablesEvent event) {}

  default void onAfterListTables(IcebergRestCatalogEvents.AfterListTablesEvent event) {}

  default void onBeforeLoadTable(IcebergRestCatalogEvents.BeforeLoadTableEvent event) {}

  default void onAfterLoadTable(IcebergRestCatalogEvents.AfterLoadTableEvent event) {}

  default void onBeforeCheckExistsTable(
      IcebergRestCatalogEvents.BeforeCheckExistsTableEvent event) {}

  default void onAfterCheckExistsTable(IcebergRestCatalogEvents.AfterCheckExistsTableEvent event) {}

  default void onBeforeDropTable(IcebergRestCatalogEvents.BeforeDropTableEvent event) {}

  default void onAfterDropTable(IcebergRestCatalogEvents.AfterDropTableEvent event) {}

  default void onBeforeRegisterTable(IcebergRestCatalogEvents.BeforeRegisterTableEvent event) {}

  default void onAfterRegisterTable(IcebergRestCatalogEvents.AfterRegisterTableEvent event) {}

  default void onBeforeRenameTable(IcebergRestCatalogEvents.BeforeRenameTableEvent event) {}

  default void onAfterRenameTable(IcebergRestCatalogEvents.AfterRenameTableEvent event) {}

  default void onBeforeUpdateTable(IcebergRestCatalogEvents.BeforeUpdateTableEvent event) {}

  default void onAfterUpdateTable(IcebergRestCatalogEvents.AfterUpdateTableEvent event) {}

  // ============ Iceberg REST View Events =============

  default void onBeforeCreateView(IcebergRestCatalogEvents.BeforeCreateViewEvent event) {}

  default void onAfterCreateView(IcebergRestCatalogEvents.AfterCreateViewEvent event) {}

  default void onBeforeCommitView(IcebergRestCatalogEvents.BeforeCommitViewEvent event) {}

  default void onAfterCommitView(IcebergRestCatalogEvents.AfterCommitViewEvent event) {}

  default void onBeforeRefreshView(IcebergRestCatalogEvents.BeforeRefreshViewEvent event) {}

  default void onAfterRefreshView(IcebergRestCatalogEvents.AfterRefreshViewEvent event) {}

  default void onBeforeListViews(IcebergRestCatalogEvents.BeforeListViewsEvent event) {}

  default void onAfterListViews(IcebergRestCatalogEvents.AfterListViewsEvent event) {}

  default void onBeforeLoadView(IcebergRestCatalogEvents.BeforeLoadViewEvent event) {}

  default void onAfterLoadView(IcebergRestCatalogEvents.AfterLoadViewEvent event) {}

  default void onBeforeCheckExistsView(IcebergRestCatalogEvents.BeforeCheckExistsViewEvent event) {}

  default void onAfterCheckExistsView(IcebergRestCatalogEvents.AfterCheckExistsViewEvent event) {}

  default void onBeforeDropView(IcebergRestCatalogEvents.BeforeDropViewEvent event) {}

  default void onAfterDropView(IcebergRestCatalogEvents.AfterDropViewEvent event) {}

  default void onBeforeRenameView(IcebergRestCatalogEvents.BeforeRenameViewEvent event) {}

  default void onAfterRenameView(IcebergRestCatalogEvents.AfterRenameViewEvent event) {}

  default void onBeforeReplaceView(IcebergRestCatalogEvents.BeforeReplaceViewEvent event) {}

  default void onAfterReplaceView(IcebergRestCatalogEvents.AfterReplaceViewEvent event) {}

  // ============ Iceberg REST Credential Events =============

  default void onBeforeLoadCredentials(IcebergRestCatalogEvents.BeforeLoadCredentialsEvent event) {}

  default void onAfterLoadCredentials(IcebergRestCatalogEvents.AfterLoadCredentialsEvent event) {}

  // ============ Iceberg REST Transaction Events =============

  default void onBeforeCommitTransaction(
      IcebergRestCatalogEvents.BeforeCommitTransactionEvent event) {}

  default void onAfterCommitTransaction(
      IcebergRestCatalogEvents.AfterCommitTransactionEvent event) {}

  // ============ Iceberg REST Notification Events =============

  default void onBeforeSendNotification(
      IcebergRestCatalogEvents.BeforeSendNotificationEvent event) {}

  default void onAfterSendNotification(IcebergRestCatalogEvents.AfterSendNotificationEvent event) {}

  // ============ Iceberg REST Configuration Events =============

  default void onBeforeGetConfig(IcebergRestCatalogEvents.BeforeGetConfigEvent event) {}

  default void onAfterGetConfig(IcebergRestCatalogEvents.AfterGetConfigEvent event) {}

  // ============= Policy Events =============

  default void onBeforeCreatePolicy(CatalogPolicyServiceEvents.BeforeCreatePolicyEvent event) {}

  default void onAfterCreatePolicy(CatalogPolicyServiceEvents.AfterCreatePolicyEvent event) {}

  default void onBeforeListPolicies(CatalogPolicyServiceEvents.BeforeListPoliciesEvent event) {}

  default void onAfterListPolicies(CatalogPolicyServiceEvents.AfterListPoliciesEvent event) {}

  default void onBeforeLoadPolicy(CatalogPolicyServiceEvents.BeforeLoadPolicyEvent event) {}

  default void onAfterLoadPolicy(CatalogPolicyServiceEvents.AfterLoadPolicyEvent event) {}

  default void onBeforeUpdatePolicy(CatalogPolicyServiceEvents.BeforeUpdatePolicyEvent event) {}

  default void onAfterUpdatePolicy(CatalogPolicyServiceEvents.AfterUpdatePolicyEvent event) {}

  default void onBeforeDropPolicy(CatalogPolicyServiceEvents.BeforeDropPolicyEvent event) {}

  default void onAfterDropPolicy(CatalogPolicyServiceEvents.AfterDropPolicyEvent event) {}

  default void onBeforeAttachPolicy(CatalogPolicyServiceEvents.BeforeAttachPolicyEvent event) {}

  default void onAfterAttachPolicy(CatalogPolicyServiceEvents.AfterAttachPolicyEvent event) {}

  default void onBeforeDetachPolicy(CatalogPolicyServiceEvents.BeforeDetachPolicyEvent event) {}

  default void onAfterDetachPolicy(CatalogPolicyServiceEvents.AfterDetachPolicyEvent event) {}

  default void onBeforeGetApplicablePolicies(
      CatalogPolicyServiceEvents.BeforeGetApplicablePoliciesEvent event) {}

  default void onAfterGetApplicablePolicies(
      CatalogPolicyServiceEvents.AfterGetApplicablePoliciesEvent event) {}

  // ============= Generic Table Events =============

  default void onBeforeCreateGenericTable(
      CatalogGenericTableServiceEvents.BeforeCreateGenericTableEvent event) {}

  default void onAfterCreateGenericTable(
      CatalogGenericTableServiceEvents.AfterCreateGenericTableEvent event) {}

  default void onBeforeDropGenericTable(
      CatalogGenericTableServiceEvents.BeforeDropGenericTableEvent event) {}

  default void onAfterDropGenericTable(
      CatalogGenericTableServiceEvents.AfterDropGenericTableEvent event) {}

  default void onBeforeListGenericTables(
      CatalogGenericTableServiceEvents.BeforeListGenericTablesEvent event) {}

  default void onAfterListGenericTables(
      CatalogGenericTableServiceEvents.AfterListGenericTablesEvent event) {}

  default void onBeforeLoadGenericTable(
      CatalogGenericTableServiceEvents.BeforeLoadGenericTableEvent event) {}

  default void onAfterLoadGenericTable(
      CatalogGenericTableServiceEvents.AfterLoadGenericTableEvent event) {}

  // ============= Task Execution Events =============

  default void onBeforeAttemptTask(BeforeAttemptTaskEvent event) {}

  default void onAfterAttemptTask(AfterAttemptTaskEvent event) {}

  // ============= Rate Limiting Events =============

  default void onBeforeLimitRequestRate(BeforeLimitRequestRateEvent event) {}
}
