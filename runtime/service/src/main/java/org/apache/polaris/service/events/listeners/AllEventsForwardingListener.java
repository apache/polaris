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
import org.apache.polaris.service.events.PolarisEvent;
import org.apache.polaris.service.events.PrincipalRolesServiceEvents;
import org.apache.polaris.service.events.PrincipalsServiceEvents;

/**
 * Base class for event listeners that with to generically forward all {@link PolarisEvent
 * PolarisEvents} to an external sink.
 *
 * <p>This design follows the Template Method pattern, centralizing shared control flow in the base
 * class while allowing subclasses to supply the event-specific behavior.
 */
public abstract class AllEventsForwardingListener implements PolarisEventListener {

  /** Subclasses implement the actual logic once, generically. */
  protected abstract void handle(PolarisEvent event);

  /** Optional filter (config-based). Default: handle all. */
  protected boolean shouldHandle(Object event) {
    return true;
  }

  @Override
  public void onAfterGetCatalog(CatalogsServiceEvents.AfterGetCatalogEvent event) {
    if (shouldHandle(event)) handle(event);
  }

  @Override
  public void onBeforeCreateCatalog(CatalogsServiceEvents.BeforeCreateCatalogEvent event) {
    if (shouldHandle(event)) handle(event);
  }

  @Override
  public void onAfterCreateCatalog(CatalogsServiceEvents.AfterCreateCatalogEvent event) {
    if (shouldHandle(event)) handle(event);
  }

  @Override
  public void onBeforeDeleteCatalog(CatalogsServiceEvents.BeforeDeleteCatalogEvent event) {
    if (shouldHandle(event)) handle(event);
  }

  @Override
  public void onAfterDeleteCatalog(CatalogsServiceEvents.AfterDeleteCatalogEvent event) {
    if (shouldHandle(event)) handle(event);
  }

  @Override
  public void onBeforeGetCatalog(CatalogsServiceEvents.BeforeGetCatalogEvent event) {
    if (shouldHandle(event)) handle(event);
  }

  @Override
  public void onBeforeUpdateCatalog(CatalogsServiceEvents.BeforeUpdateCatalogEvent event) {
    if (shouldHandle(event)) handle(event);
  }

  @Override
  public void onAfterUpdateCatalog(CatalogsServiceEvents.AfterUpdateCatalogEvent event) {
    if (shouldHandle(event)) handle(event);
  }

  @Override
  public void onBeforeListCatalogs(CatalogsServiceEvents.BeforeListCatalogsEvent event) {
    if (shouldHandle(event)) handle(event);
  }

  @Override
  public void onAfterListCatalogs(CatalogsServiceEvents.AfterListCatalogsEvent event) {
    if (shouldHandle(event)) handle(event);
  }

  @Override
  public void onBeforeCreatePrincipal(PrincipalsServiceEvents.BeforeCreatePrincipalEvent event) {
    if (shouldHandle(event)) handle(event);
  }

  @Override
  public void onAfterCreatePrincipal(PrincipalsServiceEvents.AfterCreatePrincipalEvent event) {
    if (shouldHandle(event)) handle(event);
  }

  @Override
  public void onBeforeDeletePrincipal(PrincipalsServiceEvents.BeforeDeletePrincipalEvent event) {
    if (shouldHandle(event)) handle(event);
  }

  @Override
  public void onAfterDeletePrincipal(PrincipalsServiceEvents.AfterDeletePrincipalEvent event) {
    if (shouldHandle(event)) handle(event);
  }

  @Override
  public void onBeforeGetPrincipal(PrincipalsServiceEvents.BeforeGetPrincipalEvent event) {
    if (shouldHandle(event)) handle(event);
  }

  @Override
  public void onAfterGetPrincipal(PrincipalsServiceEvents.AfterGetPrincipalEvent event) {
    if (shouldHandle(event)) handle(event);
  }

  @Override
  public void onBeforeUpdatePrincipal(PrincipalsServiceEvents.BeforeUpdatePrincipalEvent event) {
    if (shouldHandle(event)) handle(event);
  }

  @Override
  public void onAfterUpdatePrincipal(PrincipalsServiceEvents.AfterUpdatePrincipalEvent event) {
    if (shouldHandle(event)) handle(event);
  }

  @Override
  public void onBeforeRotateCredentials(
      PrincipalsServiceEvents.BeforeRotateCredentialsEvent event) {
    if (shouldHandle(event)) handle(event);
  }

  @Override
  public void onAfterRotateCredentials(PrincipalsServiceEvents.AfterRotateCredentialsEvent event) {
    if (shouldHandle(event)) handle(event);
  }

  @Override
  public void onBeforeListPrincipals(PrincipalsServiceEvents.BeforeListPrincipalsEvent event) {
    if (shouldHandle(event)) handle(event);
  }

  @Override
  public void onAfterListPrincipals(PrincipalsServiceEvents.AfterListPrincipalsEvent event) {
    if (shouldHandle(event)) handle(event);
  }

  @Override
  public void onBeforeResetCredentials(PrincipalsServiceEvents.BeforeResetCredentialsEvent event) {
    if (shouldHandle(event)) handle(event);
  }

  @Override
  public void onAfterResetCredentials(PrincipalsServiceEvents.AfterResetCredentialsEvent event) {
    if (shouldHandle(event)) handle(event);
  }

  @Override
  public void onBeforeAssignPrincipalRole(
      PrincipalsServiceEvents.BeforeAssignPrincipalRoleEvent event) {
    if (shouldHandle(event)) handle(event);
  }

  @Override
  public void onAfterAssignPrincipalRole(
      PrincipalsServiceEvents.AfterAssignPrincipalRoleEvent event) {
    if (shouldHandle(event)) handle(event);
  }

  @Override
  public void onBeforeRevokePrincipalRole(
      PrincipalsServiceEvents.BeforeRevokePrincipalRoleEvent event) {
    if (shouldHandle(event)) handle(event);
  }

  @Override
  public void onAfterRevokePrincipalRole(
      PrincipalsServiceEvents.AfterRevokePrincipalRoleEvent event) {
    if (shouldHandle(event)) handle(event);
  }

  @Override
  public void onBeforeListAssignedPrincipalRoles(
      PrincipalsServiceEvents.BeforeListAssignedPrincipalRolesEvent event) {
    if (shouldHandle(event)) handle(event);
  }

  @Override
  public void onAfterListAssignedPrincipalRoles(
      PrincipalsServiceEvents.AfterListAssignedPrincipalRolesEvent event) {
    if (shouldHandle(event)) handle(event);
  }

  @Override
  public void onBeforeCreatePrincipalRole(
      PrincipalRolesServiceEvents.BeforeCreatePrincipalRoleEvent event) {
    if (shouldHandle(event)) handle(event);
  }

  @Override
  public void onAfterCreatePrincipalRole(
      PrincipalRolesServiceEvents.AfterCreatePrincipalRoleEvent event) {
    if (shouldHandle(event)) handle(event);
  }

  @Override
  public void onBeforeDeletePrincipalRole(
      PrincipalRolesServiceEvents.BeforeDeletePrincipalRoleEvent event) {
    if (shouldHandle(event)) handle(event);
  }

  @Override
  public void onAfterDeletePrincipalRole(
      PrincipalRolesServiceEvents.AfterDeletePrincipalRoleEvent event) {
    if (shouldHandle(event)) handle(event);
  }

  @Override
  public void onBeforeGetPrincipalRole(
      PrincipalRolesServiceEvents.BeforeGetPrincipalRoleEvent event) {
    if (shouldHandle(event)) handle(event);
  }

  @Override
  public void onAfterGetPrincipalRole(
      PrincipalRolesServiceEvents.AfterGetPrincipalRoleEvent event) {
    if (shouldHandle(event)) handle(event);
  }

  @Override
  public void onBeforeUpdatePrincipalRole(
      PrincipalRolesServiceEvents.BeforeUpdatePrincipalRoleEvent event) {
    if (shouldHandle(event)) handle(event);
  }

  @Override
  public void onAfterUpdatePrincipalRole(
      PrincipalRolesServiceEvents.AfterUpdatePrincipalRoleEvent event) {
    if (shouldHandle(event)) handle(event);
  }

  @Override
  public void onBeforeListPrincipalRoles(
      PrincipalRolesServiceEvents.BeforeListPrincipalRolesEvent event) {
    if (shouldHandle(event)) handle(event);
  }

  @Override
  public void onAfterListPrincipalRoles(
      PrincipalRolesServiceEvents.AfterListPrincipalRolesEvent event) {
    if (shouldHandle(event)) handle(event);
  }

  @Override
  public void onBeforeCreateCatalogRole(CatalogsServiceEvents.BeforeCreateCatalogRoleEvent event) {
    if (shouldHandle(event)) handle(event);
  }

  @Override
  public void onAfterCreateCatalogRole(CatalogsServiceEvents.AfterCreateCatalogRoleEvent event) {
    if (shouldHandle(event)) handle(event);
  }

  @Override
  public void onBeforeDeleteCatalogRole(CatalogsServiceEvents.BeforeDeleteCatalogRoleEvent event) {
    if (shouldHandle(event)) handle(event);
  }

  @Override
  public void onAfterDeleteCatalogRole(CatalogsServiceEvents.AfterDeleteCatalogRoleEvent event) {
    if (shouldHandle(event)) handle(event);
  }

  @Override
  public void onBeforeGetCatalogRole(CatalogsServiceEvents.BeforeGetCatalogRoleEvent event) {
    if (shouldHandle(event)) handle(event);
  }

  @Override
  public void onAfterGetCatalogRole(CatalogsServiceEvents.AfterGetCatalogRoleEvent event) {
    if (shouldHandle(event)) handle(event);
  }

  @Override
  public void onBeforeUpdateCatalogRole(CatalogsServiceEvents.BeforeUpdateCatalogRoleEvent event) {
    if (shouldHandle(event)) handle(event);
  }

  @Override
  public void onAfterUpdateCatalogRole(CatalogsServiceEvents.AfterUpdateCatalogRoleEvent event) {
    if (shouldHandle(event)) handle(event);
  }

  @Override
  public void onBeforeListCatalogRoles(CatalogsServiceEvents.BeforeListCatalogRolesEvent event) {
    if (shouldHandle(event)) handle(event);
  }

  @Override
  public void onAfterListCatalogRoles(CatalogsServiceEvents.AfterListCatalogRolesEvent event) {
    if (shouldHandle(event)) handle(event);
  }

  @Override
  public void onBeforeAssignCatalogRoleToPrincipalRole(
      PrincipalRolesServiceEvents.BeforeAssignCatalogRoleToPrincipalRoleEvent event) {
    if (shouldHandle(event)) handle(event);
  }

  @Override
  public void onAfterAssignCatalogRoleToPrincipalRole(
      PrincipalRolesServiceEvents.AfterAssignCatalogRoleToPrincipalRoleEvent event) {
    if (shouldHandle(event)) handle(event);
  }

  @Override
  public void onBeforeRevokeCatalogRoleFromPrincipalRole(
      PrincipalRolesServiceEvents.BeforeRevokeCatalogRoleFromPrincipalRoleEvent event) {
    if (shouldHandle(event)) handle(event);
  }

  @Override
  public void onAfterRevokeCatalogRoleFromPrincipalRole(
      PrincipalRolesServiceEvents.AfterRevokeCatalogRoleFromPrincipalRoleEvent event) {
    if (shouldHandle(event)) handle(event);
  }

  @Override
  public void onBeforeListAssigneePrincipalsForPrincipalRole(
      PrincipalRolesServiceEvents.BeforeListAssigneePrincipalsForPrincipalRoleEvent event) {
    if (shouldHandle(event)) handle(event);
  }

  @Override
  public void onAfterListAssigneePrincipalsForPrincipalRole(
      PrincipalRolesServiceEvents.AfterListAssigneePrincipalsForPrincipalRoleEvent event) {
    if (shouldHandle(event)) handle(event);
  }

  @Override
  public void onBeforeListCatalogRolesForPrincipalRole(
      PrincipalRolesServiceEvents.BeforeListCatalogRolesForPrincipalRoleEvent event) {
    if (shouldHandle(event)) handle(event);
  }

  @Override
  public void onAfterListCatalogRolesForPrincipalRole(
      PrincipalRolesServiceEvents.AfterListCatalogRolesForPrincipalRoleEvent event) {
    if (shouldHandle(event)) handle(event);
  }

  @Override
  public void onBeforeAddGrantToCatalogRole(
      CatalogsServiceEvents.BeforeAddGrantToCatalogRoleEvent event) {
    if (shouldHandle(event)) handle(event);
  }

  @Override
  public void onAfterAddGrantToCatalogRole(
      CatalogsServiceEvents.AfterAddGrantToCatalogRoleEvent event) {
    if (shouldHandle(event)) handle(event);
  }

  @Override
  public void onBeforeRevokeGrantFromCatalogRole(
      CatalogsServiceEvents.BeforeRevokeGrantFromCatalogRoleEvent event) {
    if (shouldHandle(event)) handle(event);
  }

  @Override
  public void onAfterRevokeGrantFromCatalogRole(
      CatalogsServiceEvents.AfterRevokeGrantFromCatalogRoleEvent event) {
    if (shouldHandle(event)) handle(event);
  }

  @Override
  public void onBeforeListAssigneePrincipalRolesForCatalogRole(
      CatalogsServiceEvents.BeforeListAssigneePrincipalRolesForCatalogRoleEvent event) {
    if (shouldHandle(event)) handle(event);
  }

  @Override
  public void onAfterListAssigneePrincipalRolesForCatalogRole(
      CatalogsServiceEvents.AfterListAssigneePrincipalRolesForCatalogRoleEvent event) {
    if (shouldHandle(event)) handle(event);
  }

  @Override
  public void onBeforeListGrantsForCatalogRole(
      CatalogsServiceEvents.BeforeListGrantsForCatalogRoleEvent event) {
    if (shouldHandle(event)) handle(event);
  }

  @Override
  public void onAfterListGrantsForCatalogRole(
      CatalogsServiceEvents.AfterListGrantsForCatalogRoleEvent event) {
    if (shouldHandle(event)) handle(event);
  }

  @Override
  public void onBeforeCreateNamespace(IcebergRestCatalogEvents.BeforeCreateNamespaceEvent event) {
    if (shouldHandle(event)) handle(event);
  }

  @Override
  public void onAfterCreateNamespace(IcebergRestCatalogEvents.AfterCreateNamespaceEvent event) {
    if (shouldHandle(event)) handle(event);
  }

  @Override
  public void onBeforeListNamespaces(IcebergRestCatalogEvents.BeforeListNamespacesEvent event) {
    if (shouldHandle(event)) handle(event);
  }

  @Override
  public void onAfterListNamespaces(IcebergRestCatalogEvents.AfterListNamespacesEvent event) {
    if (shouldHandle(event)) handle(event);
  }

  @Override
  public void onBeforeLoadNamespaceMetadata(
      IcebergRestCatalogEvents.BeforeLoadNamespaceMetadataEvent event) {
    if (shouldHandle(event)) handle(event);
  }

  @Override
  public void onAfterLoadNamespaceMetadata(
      IcebergRestCatalogEvents.AfterLoadNamespaceMetadataEvent event) {
    if (shouldHandle(event)) handle(event);
  }

  @Override
  public void onBeforeCheckExistsNamespace(
      IcebergRestCatalogEvents.BeforeCheckExistsNamespaceEvent event) {
    if (shouldHandle(event)) handle(event);
  }

  @Override
  public void onAfterCheckExistsNamespace(
      IcebergRestCatalogEvents.AfterCheckExistsNamespaceEvent event) {
    if (shouldHandle(event)) handle(event);
  }

  @Override
  public void onBeforeDropNamespace(IcebergRestCatalogEvents.BeforeDropNamespaceEvent event) {
    if (shouldHandle(event)) handle(event);
  }

  @Override
  public void onAfterDropNamespace(IcebergRestCatalogEvents.AfterDropNamespaceEvent event) {
    if (shouldHandle(event)) handle(event);
  }

  @Override
  public void onBeforeUpdateNamespaceProperties(
      IcebergRestCatalogEvents.BeforeUpdateNamespacePropertiesEvent event) {
    if (shouldHandle(event)) handle(event);
  }

  @Override
  public void onAfterUpdateNamespaceProperties(
      IcebergRestCatalogEvents.AfterUpdateNamespacePropertiesEvent event) {
    if (shouldHandle(event)) handle(event);
  }

  @Override
  public void onBeforeCreateTable(IcebergRestCatalogEvents.BeforeCreateTableEvent event) {
    if (shouldHandle(event)) handle(event);
  }

  @Override
  public void onAfterCreateTable(IcebergRestCatalogEvents.AfterCreateTableEvent event) {
    if (shouldHandle(event)) handle(event);
  }

  @Override
  public void onBeforeCommitTable(IcebergRestCatalogEvents.BeforeCommitTableEvent event) {
    if (shouldHandle(event)) handle(event);
  }

  @Override
  public void onAfterCommitTable(IcebergRestCatalogEvents.AfterCommitTableEvent event) {
    if (shouldHandle(event)) handle(event);
  }

  @Override
  public void onBeforeRefreshTable(IcebergRestCatalogEvents.BeforeRefreshTableEvent event) {
    if (shouldHandle(event)) handle(event);
  }

  @Override
  public void onAfterRefreshTable(IcebergRestCatalogEvents.AfterRefreshTableEvent event) {
    if (shouldHandle(event)) handle(event);
  }

  @Override
  public void onBeforeListTables(IcebergRestCatalogEvents.BeforeListTablesEvent event) {
    if (shouldHandle(event)) handle(event);
  }

  @Override
  public void onAfterListTables(IcebergRestCatalogEvents.AfterListTablesEvent event) {
    if (shouldHandle(event)) handle(event);
  }

  @Override
  public void onBeforeLoadTable(IcebergRestCatalogEvents.BeforeLoadTableEvent event) {
    if (shouldHandle(event)) handle(event);
  }

  @Override
  public void onAfterLoadTable(IcebergRestCatalogEvents.AfterLoadTableEvent event) {
    if (shouldHandle(event)) handle(event);
  }

  @Override
  public void onBeforeCheckExistsTable(IcebergRestCatalogEvents.BeforeCheckExistsTableEvent event) {
    if (shouldHandle(event)) handle(event);
  }

  @Override
  public void onAfterCheckExistsTable(IcebergRestCatalogEvents.AfterCheckExistsTableEvent event) {
    if (shouldHandle(event)) handle(event);
  }

  @Override
  public void onBeforeDropTable(IcebergRestCatalogEvents.BeforeDropTableEvent event) {
    if (shouldHandle(event)) handle(event);
  }

  @Override
  public void onAfterDropTable(IcebergRestCatalogEvents.AfterDropTableEvent event) {
    if (shouldHandle(event)) handle(event);
  }

  @Override
  public void onBeforeRegisterTable(IcebergRestCatalogEvents.BeforeRegisterTableEvent event) {
    if (shouldHandle(event)) handle(event);
  }

  @Override
  public void onAfterRegisterTable(IcebergRestCatalogEvents.AfterRegisterTableEvent event) {
    if (shouldHandle(event)) handle(event);
  }

  @Override
  public void onBeforeRenameTable(IcebergRestCatalogEvents.BeforeRenameTableEvent event) {
    if (shouldHandle(event)) handle(event);
  }

  @Override
  public void onAfterRenameTable(IcebergRestCatalogEvents.AfterRenameTableEvent event) {
    if (shouldHandle(event)) handle(event);
  }

  @Override
  public void onBeforeUpdateTable(IcebergRestCatalogEvents.BeforeUpdateTableEvent event) {
    if (shouldHandle(event)) handle(event);
  }

  @Override
  public void onAfterUpdateTable(IcebergRestCatalogEvents.AfterUpdateTableEvent event) {
    if (shouldHandle(event)) handle(event);
  }

  @Override
  public void onBeforeCreateView(IcebergRestCatalogEvents.BeforeCreateViewEvent event) {
    if (shouldHandle(event)) handle(event);
  }

  @Override
  public void onAfterCreateView(IcebergRestCatalogEvents.AfterCreateViewEvent event) {
    if (shouldHandle(event)) handle(event);
  }

  @Override
  public void onBeforeCommitView(IcebergRestCatalogEvents.BeforeCommitViewEvent event) {
    if (shouldHandle(event)) handle(event);
  }

  @Override
  public void onAfterCommitView(IcebergRestCatalogEvents.AfterCommitViewEvent event) {
    if (shouldHandle(event)) handle(event);
  }

  @Override
  public void onBeforeRefreshView(IcebergRestCatalogEvents.BeforeRefreshViewEvent event) {
    if (shouldHandle(event)) handle(event);
  }

  @Override
  public void onAfterRefreshView(IcebergRestCatalogEvents.AfterRefreshViewEvent event) {
    if (shouldHandle(event)) handle(event);
  }

  @Override
  public void onBeforeListViews(IcebergRestCatalogEvents.BeforeListViewsEvent event) {
    if (shouldHandle(event)) handle(event);
  }

  @Override
  public void onAfterListViews(IcebergRestCatalogEvents.AfterListViewsEvent event) {
    if (shouldHandle(event)) handle(event);
  }

  @Override
  public void onBeforeLoadView(IcebergRestCatalogEvents.BeforeLoadViewEvent event) {
    if (shouldHandle(event)) handle(event);
  }

  @Override
  public void onAfterLoadView(IcebergRestCatalogEvents.AfterLoadViewEvent event) {
    if (shouldHandle(event)) handle(event);
  }

  @Override
  public void onBeforeCheckExistsView(IcebergRestCatalogEvents.BeforeCheckExistsViewEvent event) {
    if (shouldHandle(event)) handle(event);
  }

  @Override
  public void onAfterCheckExistsView(IcebergRestCatalogEvents.AfterCheckExistsViewEvent event) {
    if (shouldHandle(event)) handle(event);
  }

  @Override
  public void onBeforeDropView(IcebergRestCatalogEvents.BeforeDropViewEvent event) {
    if (shouldHandle(event)) handle(event);
  }

  @Override
  public void onAfterDropView(IcebergRestCatalogEvents.AfterDropViewEvent event) {
    if (shouldHandle(event)) handle(event);
  }

  @Override
  public void onBeforeRenameView(IcebergRestCatalogEvents.BeforeRenameViewEvent event) {
    if (shouldHandle(event)) handle(event);
  }

  @Override
  public void onAfterRenameView(IcebergRestCatalogEvents.AfterRenameViewEvent event) {
    if (shouldHandle(event)) handle(event);
  }

  @Override
  public void onBeforeReplaceView(IcebergRestCatalogEvents.BeforeReplaceViewEvent event) {
    if (shouldHandle(event)) handle(event);
  }

  @Override
  public void onAfterReplaceView(IcebergRestCatalogEvents.AfterReplaceViewEvent event) {
    if (shouldHandle(event)) handle(event);
  }

  @Override
  public void onBeforeLoadCredentials(IcebergRestCatalogEvents.BeforeLoadCredentialsEvent event) {
    if (shouldHandle(event)) handle(event);
  }

  @Override
  public void onAfterLoadCredentials(IcebergRestCatalogEvents.AfterLoadCredentialsEvent event) {
    if (shouldHandle(event)) handle(event);
  }

  @Override
  public void onBeforeCommitTransaction(
      IcebergRestCatalogEvents.BeforeCommitTransactionEvent event) {
    if (shouldHandle(event)) handle(event);
  }

  @Override
  public void onAfterCommitTransaction(IcebergRestCatalogEvents.AfterCommitTransactionEvent event) {
    if (shouldHandle(event)) handle(event);
  }

  @Override
  public void onBeforeSendNotification(IcebergRestCatalogEvents.BeforeSendNotificationEvent event) {
    if (shouldHandle(event)) handle(event);
  }

  @Override
  public void onAfterSendNotification(IcebergRestCatalogEvents.AfterSendNotificationEvent event) {
    if (shouldHandle(event)) handle(event);
  }

  @Override
  public void onBeforeGetConfig(IcebergRestCatalogEvents.BeforeGetConfigEvent event) {
    if (shouldHandle(event)) handle(event);
  }

  @Override
  public void onAfterGetConfig(IcebergRestCatalogEvents.AfterGetConfigEvent event) {
    if (shouldHandle(event)) handle(event);
  }

  @Override
  public void onBeforeCreatePolicy(CatalogPolicyServiceEvents.BeforeCreatePolicyEvent event) {
    if (shouldHandle(event)) handle(event);
  }

  @Override
  public void onAfterCreatePolicy(CatalogPolicyServiceEvents.AfterCreatePolicyEvent event) {
    if (shouldHandle(event)) handle(event);
  }

  @Override
  public void onBeforeListPolicies(CatalogPolicyServiceEvents.BeforeListPoliciesEvent event) {
    if (shouldHandle(event)) handle(event);
  }

  @Override
  public void onAfterListPolicies(CatalogPolicyServiceEvents.AfterListPoliciesEvent event) {
    if (shouldHandle(event)) handle(event);
  }

  @Override
  public void onBeforeLoadPolicy(CatalogPolicyServiceEvents.BeforeLoadPolicyEvent event) {
    if (shouldHandle(event)) handle(event);
  }

  @Override
  public void onAfterLoadPolicy(CatalogPolicyServiceEvents.AfterLoadPolicyEvent event) {
    if (shouldHandle(event)) handle(event);
  }

  @Override
  public void onBeforeUpdatePolicy(CatalogPolicyServiceEvents.BeforeUpdatePolicyEvent event) {
    if (shouldHandle(event)) handle(event);
  }

  @Override
  public void onAfterUpdatePolicy(CatalogPolicyServiceEvents.AfterUpdatePolicyEvent event) {
    if (shouldHandle(event)) handle(event);
  }

  @Override
  public void onBeforeDropPolicy(CatalogPolicyServiceEvents.BeforeDropPolicyEvent event) {
    if (shouldHandle(event)) handle(event);
  }

  @Override
  public void onAfterDropPolicy(CatalogPolicyServiceEvents.AfterDropPolicyEvent event) {
    if (shouldHandle(event)) handle(event);
  }

  @Override
  public void onBeforeAttachPolicy(CatalogPolicyServiceEvents.BeforeAttachPolicyEvent event) {
    if (shouldHandle(event)) handle(event);
  }

  @Override
  public void onAfterAttachPolicy(CatalogPolicyServiceEvents.AfterAttachPolicyEvent event) {
    if (shouldHandle(event)) handle(event);
  }

  @Override
  public void onBeforeDetachPolicy(CatalogPolicyServiceEvents.BeforeDetachPolicyEvent event) {
    if (shouldHandle(event)) handle(event);
  }

  @Override
  public void onAfterDetachPolicy(CatalogPolicyServiceEvents.AfterDetachPolicyEvent event) {
    if (shouldHandle(event)) handle(event);
  }

  @Override
  public void onBeforeGetApplicablePolicies(
      CatalogPolicyServiceEvents.BeforeGetApplicablePoliciesEvent event) {
    if (shouldHandle(event)) handle(event);
  }

  @Override
  public void onAfterGetApplicablePolicies(
      CatalogPolicyServiceEvents.AfterGetApplicablePoliciesEvent event) {
    if (shouldHandle(event)) handle(event);
  }

  @Override
  public void onBeforeCreateGenericTable(
      CatalogGenericTableServiceEvents.BeforeCreateGenericTableEvent event) {
    if (shouldHandle(event)) handle(event);
  }

  @Override
  public void onAfterCreateGenericTable(
      CatalogGenericTableServiceEvents.AfterCreateGenericTableEvent event) {
    if (shouldHandle(event)) handle(event);
  }

  @Override
  public void onBeforeDropGenericTable(
      CatalogGenericTableServiceEvents.BeforeDropGenericTableEvent event) {
    if (shouldHandle(event)) handle(event);
  }

  @Override
  public void onAfterDropGenericTable(
      CatalogGenericTableServiceEvents.AfterDropGenericTableEvent event) {
    if (shouldHandle(event)) handle(event);
  }

  @Override
  public void onBeforeListGenericTables(
      CatalogGenericTableServiceEvents.BeforeListGenericTablesEvent event) {
    if (shouldHandle(event)) handle(event);
  }

  @Override
  public void onAfterListGenericTables(
      CatalogGenericTableServiceEvents.AfterListGenericTablesEvent event) {
    if (shouldHandle(event)) handle(event);
  }

  @Override
  public void onBeforeLoadGenericTable(
      CatalogGenericTableServiceEvents.BeforeLoadGenericTableEvent event) {
    if (shouldHandle(event)) handle(event);
  }

  @Override
  public void onAfterLoadGenericTable(
      CatalogGenericTableServiceEvents.AfterLoadGenericTableEvent event) {
    if (shouldHandle(event)) handle(event);
  }

  @Override
  public void onBeforeAttemptTask(BeforeAttemptTaskEvent event) {
    if (shouldHandle(event)) handle(event);
  }

  @Override
  public void onAfterAttemptTask(AfterAttemptTaskEvent event) {
    if (shouldHandle(event)) handle(event);
  }

  @Override
  public void onBeforeLimitRequestRate(BeforeLimitRequestRateEvent event) {
    if (shouldHandle(event)) handle(event);
  }
}
