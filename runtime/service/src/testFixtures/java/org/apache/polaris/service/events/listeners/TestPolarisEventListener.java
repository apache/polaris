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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
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

/** Test event listener that stores all emitted events forever. */
public class TestPolarisEventListener implements PolarisEventListener {
  private final Map<Class<? extends PolarisEvent>, PolarisEvent> latestEvents =
      new ConcurrentHashMap<>();

  private void recordEvent(PolarisEvent event) {
    latestEvents.put(event.getClass(), event);
  }

  public void clear() {
    latestEvents.clear();
  }

  public <T> T getLatest(Class<T> type) {
    var latest = latestEvents.get(type);
    if (latest == null) {
      throw new IllegalStateException("No event of type " + type + " recorded");
    }
    return type.cast(latest);
  }

  @Override
  public void onBeforeCreateCatalog(CatalogsServiceEvents.BeforeCreateCatalogEvent event) {
    recordEvent(event);
  }

  @Override
  public void onAfterCreateCatalog(CatalogsServiceEvents.AfterCreateCatalogEvent event) {
    recordEvent(event);
  }

  @Override
  public void onBeforeDeleteCatalog(CatalogsServiceEvents.BeforeDeleteCatalogEvent event) {
    recordEvent(event);
  }

  @Override
  public void onAfterDeleteCatalog(CatalogsServiceEvents.AfterDeleteCatalogEvent event) {
    recordEvent(event);
  }

  @Override
  public void onBeforeGetCatalog(CatalogsServiceEvents.BeforeGetCatalogEvent event) {
    recordEvent(event);
  }

  @Override
  public void onAfterGetCatalog(CatalogsServiceEvents.AfterGetCatalogEvent event) {
    recordEvent(event);
  }

  @Override
  public void onBeforeUpdateCatalog(CatalogsServiceEvents.BeforeUpdateCatalogEvent event) {
    recordEvent(event);
  }

  @Override
  public void onAfterUpdateCatalog(CatalogsServiceEvents.AfterUpdateCatalogEvent event) {
    recordEvent(event);
  }

  @Override
  public void onBeforeListCatalogs(CatalogsServiceEvents.BeforeListCatalogsEvent event) {
    recordEvent(event);
  }

  @Override
  public void onAfterListCatalogs(CatalogsServiceEvents.AfterListCatalogsEvent event) {
    recordEvent(event);
  }

  @Override
  public void onBeforeCreatePrincipal(PrincipalsServiceEvents.BeforeCreatePrincipalEvent event) {
    recordEvent(event);
  }

  @Override
  public void onAfterCreatePrincipal(PrincipalsServiceEvents.AfterCreatePrincipalEvent event) {
    recordEvent(event);
  }

  @Override
  public void onBeforeDeletePrincipal(PrincipalsServiceEvents.BeforeDeletePrincipalEvent event) {
    recordEvent(event);
  }

  @Override
  public void onAfterDeletePrincipal(PrincipalsServiceEvents.AfterDeletePrincipalEvent event) {
    recordEvent(event);
  }

  @Override
  public void onBeforeGetPrincipal(PrincipalsServiceEvents.BeforeGetPrincipalEvent event) {
    recordEvent(event);
  }

  @Override
  public void onAfterGetPrincipal(PrincipalsServiceEvents.AfterGetPrincipalEvent event) {
    recordEvent(event);
  }

  @Override
  public void onBeforeUpdatePrincipal(PrincipalsServiceEvents.BeforeUpdatePrincipalEvent event) {
    recordEvent(event);
  }

  @Override
  public void onAfterUpdatePrincipal(PrincipalsServiceEvents.AfterUpdatePrincipalEvent event) {
    recordEvent(event);
  }

  @Override
  public void onBeforeRotateCredentials(
      PrincipalsServiceEvents.BeforeRotateCredentialsEvent event) {
    recordEvent(event);
  }

  @Override
  public void onAfterRotateCredentials(PrincipalsServiceEvents.AfterRotateCredentialsEvent event) {
    recordEvent(event);
  }

  @Override
  public void onBeforeListPrincipals(PrincipalsServiceEvents.BeforeListPrincipalsEvent event) {
    recordEvent(event);
  }

  @Override
  public void onAfterListPrincipals(PrincipalsServiceEvents.AfterListPrincipalsEvent event) {
    recordEvent(event);
  }

  @Override
  public void onBeforeResetCredentials(PrincipalsServiceEvents.BeforeResetCredentialsEvent event) {
    recordEvent(event);
  }

  @Override
  public void onAfterResetCredentials(PrincipalsServiceEvents.AfterResetCredentialsEvent event) {
    recordEvent(event);
  }

  @Override
  public void onBeforeAssignPrincipalRole(
      PrincipalsServiceEvents.BeforeAssignPrincipalRoleEvent event) {
    recordEvent(event);
  }

  @Override
  public void onAfterAssignPrincipalRole(
      PrincipalsServiceEvents.AfterAssignPrincipalRoleEvent event) {
    recordEvent(event);
  }

  @Override
  public void onBeforeRevokePrincipalRole(
      PrincipalsServiceEvents.BeforeRevokePrincipalRoleEvent event) {
    recordEvent(event);
  }

  @Override
  public void onAfterRevokePrincipalRole(
      PrincipalsServiceEvents.AfterRevokePrincipalRoleEvent event) {
    recordEvent(event);
  }

  @Override
  public void onBeforeListAssignedPrincipalRoles(
      PrincipalsServiceEvents.BeforeListAssignedPrincipalRolesEvent event) {
    recordEvent(event);
  }

  @Override
  public void onAfterListAssignedPrincipalRoles(
      PrincipalsServiceEvents.AfterListAssignedPrincipalRolesEvent event) {
    recordEvent(event);
  }

  @Override
  public void onBeforeCreatePrincipalRole(
      PrincipalRolesServiceEvents.BeforeCreatePrincipalRoleEvent event) {
    recordEvent(event);
  }

  @Override
  public void onAfterCreatePrincipalRole(
      PrincipalRolesServiceEvents.AfterCreatePrincipalRoleEvent event) {
    recordEvent(event);
  }

  @Override
  public void onBeforeDeletePrincipalRole(
      PrincipalRolesServiceEvents.BeforeDeletePrincipalRoleEvent event) {
    recordEvent(event);
  }

  @Override
  public void onAfterDeletePrincipalRole(
      PrincipalRolesServiceEvents.AfterDeletePrincipalRoleEvent event) {
    recordEvent(event);
  }

  @Override
  public void onBeforeGetPrincipalRole(
      PrincipalRolesServiceEvents.BeforeGetPrincipalRoleEvent event) {
    recordEvent(event);
  }

  @Override
  public void onAfterGetPrincipalRole(
      PrincipalRolesServiceEvents.AfterGetPrincipalRoleEvent event) {
    recordEvent(event);
  }

  @Override
  public void onBeforeUpdatePrincipalRole(
      PrincipalRolesServiceEvents.BeforeUpdatePrincipalRoleEvent event) {
    recordEvent(event);
  }

  @Override
  public void onAfterUpdatePrincipalRole(
      PrincipalRolesServiceEvents.AfterUpdatePrincipalRoleEvent event) {
    recordEvent(event);
  }

  @Override
  public void onBeforeListPrincipalRoles(
      PrincipalRolesServiceEvents.BeforeListPrincipalRolesEvent event) {
    recordEvent(event);
  }

  @Override
  public void onAfterListPrincipalRoles(
      PrincipalRolesServiceEvents.AfterListPrincipalRolesEvent event) {
    recordEvent(event);
  }

  @Override
  public void onBeforeCreateCatalogRole(CatalogsServiceEvents.BeforeCreateCatalogRoleEvent event) {
    recordEvent(event);
  }

  @Override
  public void onAfterCreateCatalogRole(CatalogsServiceEvents.AfterCreateCatalogRoleEvent event) {
    recordEvent(event);
  }

  @Override
  public void onBeforeDeleteCatalogRole(CatalogsServiceEvents.BeforeDeleteCatalogRoleEvent event) {
    recordEvent(event);
  }

  @Override
  public void onAfterDeleteCatalogRole(CatalogsServiceEvents.AfterDeleteCatalogRoleEvent event) {
    recordEvent(event);
  }

  @Override
  public void onBeforeGetCatalogRole(CatalogsServiceEvents.BeforeGetCatalogRoleEvent event) {
    recordEvent(event);
  }

  @Override
  public void onAfterGetCatalogRole(CatalogsServiceEvents.AfterGetCatalogRoleEvent event) {
    recordEvent(event);
  }

  @Override
  public void onBeforeUpdateCatalogRole(CatalogsServiceEvents.BeforeUpdateCatalogRoleEvent event) {
    recordEvent(event);
  }

  @Override
  public void onAfterUpdateCatalogRole(CatalogsServiceEvents.AfterUpdateCatalogRoleEvent event) {
    recordEvent(event);
  }

  @Override
  public void onBeforeListCatalogRoles(CatalogsServiceEvents.BeforeListCatalogRolesEvent event) {
    recordEvent(event);
  }

  @Override
  public void onAfterListCatalogRoles(CatalogsServiceEvents.AfterListCatalogRolesEvent event) {
    recordEvent(event);
  }

  @Override
  public void onBeforeAssignCatalogRoleToPrincipalRole(
      PrincipalRolesServiceEvents.BeforeAssignCatalogRoleToPrincipalRoleEvent event) {
    recordEvent(event);
  }

  @Override
  public void onAfterAssignCatalogRoleToPrincipalRole(
      PrincipalRolesServiceEvents.AfterAssignCatalogRoleToPrincipalRoleEvent event) {
    recordEvent(event);
  }

  @Override
  public void onBeforeRevokeCatalogRoleFromPrincipalRole(
      PrincipalRolesServiceEvents.BeforeRevokeCatalogRoleFromPrincipalRoleEvent event) {
    recordEvent(event);
  }

  @Override
  public void onAfterRevokeCatalogRoleFromPrincipalRole(
      PrincipalRolesServiceEvents.AfterRevokeCatalogRoleFromPrincipalRoleEvent event) {
    recordEvent(event);
  }

  @Override
  public void onBeforeListAssigneePrincipalsForPrincipalRole(
      PrincipalRolesServiceEvents.BeforeListAssigneePrincipalsForPrincipalRoleEvent event) {
    recordEvent(event);
  }

  @Override
  public void onAfterListAssigneePrincipalsForPrincipalRole(
      PrincipalRolesServiceEvents.AfterListAssigneePrincipalsForPrincipalRoleEvent event) {
    recordEvent(event);
  }

  @Override
  public void onBeforeListCatalogRolesForPrincipalRole(
      PrincipalRolesServiceEvents.BeforeListCatalogRolesForPrincipalRoleEvent event) {
    recordEvent(event);
  }

  @Override
  public void onAfterListCatalogRolesForPrincipalRole(
      PrincipalRolesServiceEvents.AfterListCatalogRolesForPrincipalRoleEvent event) {
    recordEvent(event);
  }

  @Override
  public void onBeforeAddGrantToCatalogRole(
      CatalogsServiceEvents.BeforeAddGrantToCatalogRoleEvent event) {
    recordEvent(event);
  }

  @Override
  public void onAfterAddGrantToCatalogRole(
      CatalogsServiceEvents.AfterAddGrantToCatalogRoleEvent event) {
    recordEvent(event);
  }

  @Override
  public void onBeforeRevokeGrantFromCatalogRole(
      CatalogsServiceEvents.BeforeRevokeGrantFromCatalogRoleEvent event) {
    recordEvent(event);
  }

  @Override
  public void onAfterRevokeGrantFromCatalogRole(
      CatalogsServiceEvents.AfterRevokeGrantFromCatalogRoleEvent event) {
    recordEvent(event);
  }

  @Override
  public void onBeforeListAssigneePrincipalRolesForCatalogRole(
      CatalogsServiceEvents.BeforeListAssigneePrincipalRolesForCatalogRoleEvent event) {
    recordEvent(event);
  }

  @Override
  public void onAfterListAssigneePrincipalRolesForCatalogRole(
      CatalogsServiceEvents.AfterListAssigneePrincipalRolesForCatalogRoleEvent event) {
    recordEvent(event);
  }

  @Override
  public void onBeforeListGrantsForCatalogRole(
      CatalogsServiceEvents.BeforeListGrantsForCatalogRoleEvent event) {
    recordEvent(event);
  }

  @Override
  public void onAfterListGrantsForCatalogRole(
      CatalogsServiceEvents.AfterListGrantsForCatalogRoleEvent event) {
    recordEvent(event);
  }

  @Override
  public void onBeforeCreateNamespace(IcebergRestCatalogEvents.BeforeCreateNamespaceEvent event) {
    recordEvent(event);
  }

  @Override
  public void onAfterCreateNamespace(IcebergRestCatalogEvents.AfterCreateNamespaceEvent event) {
    recordEvent(event);
  }

  @Override
  public void onBeforeListNamespaces(IcebergRestCatalogEvents.BeforeListNamespacesEvent event) {
    recordEvent(event);
  }

  @Override
  public void onAfterListNamespaces(IcebergRestCatalogEvents.AfterListNamespacesEvent event) {
    recordEvent(event);
  }

  @Override
  public void onBeforeLoadNamespaceMetadata(
      IcebergRestCatalogEvents.BeforeLoadNamespaceMetadataEvent event) {
    recordEvent(event);
  }

  @Override
  public void onAfterLoadNamespaceMetadata(
      IcebergRestCatalogEvents.AfterLoadNamespaceMetadataEvent event) {
    recordEvent(event);
  }

  @Override
  public void onBeforeCheckExistsNamespace(
      IcebergRestCatalogEvents.BeforeCheckExistsNamespaceEvent event) {
    recordEvent(event);
  }

  @Override
  public void onAfterCheckExistsNamespace(
      IcebergRestCatalogEvents.AfterCheckExistsNamespaceEvent event) {
    recordEvent(event);
  }

  @Override
  public void onBeforeDropNamespace(IcebergRestCatalogEvents.BeforeDropNamespaceEvent event) {
    recordEvent(event);
  }

  @Override
  public void onAfterDropNamespace(IcebergRestCatalogEvents.AfterDropNamespaceEvent event) {
    recordEvent(event);
  }

  @Override
  public void onBeforeUpdateNamespaceProperties(
      IcebergRestCatalogEvents.BeforeUpdateNamespacePropertiesEvent event) {
    recordEvent(event);
  }

  @Override
  public void onAfterUpdateNamespaceProperties(
      IcebergRestCatalogEvents.AfterUpdateNamespacePropertiesEvent event) {
    recordEvent(event);
  }

  @Override
  public void onBeforeCreateTable(IcebergRestCatalogEvents.BeforeCreateTableEvent event) {
    recordEvent(event);
  }

  @Override
  public void onAfterCreateTable(IcebergRestCatalogEvents.AfterCreateTableEvent event) {
    recordEvent(event);
  }

  @Override
  public void onBeforeRefreshTable(IcebergRestCatalogEvents.BeforeRefreshTableEvent event) {
    recordEvent(event);
  }

  @Override
  public void onAfterRefreshTable(IcebergRestCatalogEvents.AfterRefreshTableEvent event) {
    recordEvent(event);
  }

  @Override
  public void onBeforeListTables(IcebergRestCatalogEvents.BeforeListTablesEvent event) {
    recordEvent(event);
  }

  @Override
  public void onAfterListTables(IcebergRestCatalogEvents.AfterListTablesEvent event) {
    recordEvent(event);
  }

  @Override
  public void onBeforeLoadTable(IcebergRestCatalogEvents.BeforeLoadTableEvent event) {
    recordEvent(event);
  }

  @Override
  public void onAfterLoadTable(IcebergRestCatalogEvents.AfterLoadTableEvent event) {
    recordEvent(event);
  }

  @Override
  public void onBeforeCheckExistsTable(IcebergRestCatalogEvents.BeforeCheckExistsTableEvent event) {
    recordEvent(event);
  }

  @Override
  public void onAfterCheckExistsTable(IcebergRestCatalogEvents.AfterCheckExistsTableEvent event) {
    recordEvent(event);
  }

  @Override
  public void onBeforeDropTable(IcebergRestCatalogEvents.BeforeDropTableEvent event) {
    recordEvent(event);
  }

  @Override
  public void onAfterDropTable(IcebergRestCatalogEvents.AfterDropTableEvent event) {
    recordEvent(event);
  }

  @Override
  public void onBeforeRegisterTable(IcebergRestCatalogEvents.BeforeRegisterTableEvent event) {
    recordEvent(event);
  }

  @Override
  public void onAfterRegisterTable(IcebergRestCatalogEvents.AfterRegisterTableEvent event) {
    recordEvent(event);
  }

  @Override
  public void onBeforeRenameTable(IcebergRestCatalogEvents.BeforeRenameTableEvent event) {
    recordEvent(event);
  }

  @Override
  public void onAfterRenameTable(IcebergRestCatalogEvents.AfterRenameTableEvent event) {
    recordEvent(event);
  }

  @Override
  public void onBeforeUpdateTable(IcebergRestCatalogEvents.BeforeUpdateTableEvent event) {
    recordEvent(event);
  }

  @Override
  public void onAfterUpdateTable(IcebergRestCatalogEvents.AfterUpdateTableEvent event) {
    recordEvent(event);
  }

  @Override
  public void onBeforeCreateView(IcebergRestCatalogEvents.BeforeCreateViewEvent event) {
    recordEvent(event);
  }

  @Override
  public void onAfterCreateView(IcebergRestCatalogEvents.AfterCreateViewEvent event) {
    recordEvent(event);
  }

  @Override
  public void onBeforeCommitView(IcebergRestCatalogEvents.BeforeCommitViewEvent event) {
    recordEvent(event);
  }

  @Override
  public void onAfterCommitView(IcebergRestCatalogEvents.AfterCommitViewEvent event) {
    recordEvent(event);
  }

  @Override
  public void onBeforeRefreshView(IcebergRestCatalogEvents.BeforeRefreshViewEvent event) {
    recordEvent(event);
  }

  @Override
  public void onAfterRefreshView(IcebergRestCatalogEvents.AfterRefreshViewEvent event) {
    recordEvent(event);
  }

  @Override
  public void onBeforeListViews(IcebergRestCatalogEvents.BeforeListViewsEvent event) {
    recordEvent(event);
  }

  @Override
  public void onAfterListViews(IcebergRestCatalogEvents.AfterListViewsEvent event) {
    recordEvent(event);
  }

  @Override
  public void onBeforeLoadView(IcebergRestCatalogEvents.BeforeLoadViewEvent event) {
    recordEvent(event);
  }

  @Override
  public void onAfterLoadView(IcebergRestCatalogEvents.AfterLoadViewEvent event) {
    recordEvent(event);
  }

  @Override
  public void onBeforeCheckExistsView(IcebergRestCatalogEvents.BeforeCheckExistsViewEvent event) {
    recordEvent(event);
  }

  @Override
  public void onAfterCheckExistsView(IcebergRestCatalogEvents.AfterCheckExistsViewEvent event) {
    recordEvent(event);
  }

  @Override
  public void onBeforeDropView(IcebergRestCatalogEvents.BeforeDropViewEvent event) {
    recordEvent(event);
  }

  @Override
  public void onAfterDropView(IcebergRestCatalogEvents.AfterDropViewEvent event) {
    recordEvent(event);
  }

  @Override
  public void onBeforeRenameView(IcebergRestCatalogEvents.BeforeRenameViewEvent event) {
    recordEvent(event);
  }

  @Override
  public void onAfterRenameView(IcebergRestCatalogEvents.AfterRenameViewEvent event) {
    recordEvent(event);
  }

  @Override
  public void onBeforeReplaceView(IcebergRestCatalogEvents.BeforeReplaceViewEvent event) {
    recordEvent(event);
  }

  @Override
  public void onAfterReplaceView(IcebergRestCatalogEvents.AfterReplaceViewEvent event) {
    recordEvent(event);
  }

  @Override
  public void onBeforeLoadCredentials(IcebergRestCatalogEvents.BeforeLoadCredentialsEvent event) {
    recordEvent(event);
  }

  @Override
  public void onAfterLoadCredentials(IcebergRestCatalogEvents.AfterLoadCredentialsEvent event) {
    recordEvent(event);
  }

  @Override
  public void onBeforeCommitTransaction(
      IcebergRestCatalogEvents.BeforeCommitTransactionEvent event) {
    recordEvent(event);
  }

  @Override
  public void onAfterCommitTransaction(IcebergRestCatalogEvents.AfterCommitTransactionEvent event) {
    recordEvent(event);
  }

  @Override
  public void onBeforeSendNotification(IcebergRestCatalogEvents.BeforeSendNotificationEvent event) {
    recordEvent(event);
  }

  @Override
  public void onAfterSendNotification(IcebergRestCatalogEvents.AfterSendNotificationEvent event) {
    recordEvent(event);
  }

  @Override
  public void onBeforeGetConfig(IcebergRestCatalogEvents.BeforeGetConfigEvent event) {
    recordEvent(event);
  }

  @Override
  public void onAfterGetConfig(IcebergRestCatalogEvents.AfterGetConfigEvent event) {
    recordEvent(event);
  }

  @Override
  public void onBeforeCreatePolicy(CatalogPolicyServiceEvents.BeforeCreatePolicyEvent event) {
    recordEvent(event);
  }

  @Override
  public void onAfterCreatePolicy(CatalogPolicyServiceEvents.AfterCreatePolicyEvent event) {
    recordEvent(event);
  }

  @Override
  public void onBeforeListPolicies(CatalogPolicyServiceEvents.BeforeListPoliciesEvent event) {
    recordEvent(event);
  }

  @Override
  public void onAfterListPolicies(CatalogPolicyServiceEvents.AfterListPoliciesEvent event) {
    recordEvent(event);
  }

  @Override
  public void onBeforeLoadPolicy(CatalogPolicyServiceEvents.BeforeLoadPolicyEvent event) {
    recordEvent(event);
  }

  @Override
  public void onAfterLoadPolicy(CatalogPolicyServiceEvents.AfterLoadPolicyEvent event) {
    recordEvent(event);
  }

  @Override
  public void onBeforeUpdatePolicy(CatalogPolicyServiceEvents.BeforeUpdatePolicyEvent event) {
    recordEvent(event);
  }

  @Override
  public void onAfterUpdatePolicy(CatalogPolicyServiceEvents.AfterUpdatePolicyEvent event) {
    recordEvent(event);
  }

  @Override
  public void onBeforeDropPolicy(CatalogPolicyServiceEvents.BeforeDropPolicyEvent event) {
    recordEvent(event);
  }

  @Override
  public void onAfterDropPolicy(CatalogPolicyServiceEvents.AfterDropPolicyEvent event) {
    recordEvent(event);
  }

  @Override
  public void onBeforeAttachPolicy(CatalogPolicyServiceEvents.BeforeAttachPolicyEvent event) {
    recordEvent(event);
  }

  @Override
  public void onAfterAttachPolicy(CatalogPolicyServiceEvents.AfterAttachPolicyEvent event) {
    recordEvent(event);
  }

  @Override
  public void onBeforeDetachPolicy(CatalogPolicyServiceEvents.BeforeDetachPolicyEvent event) {
    recordEvent(event);
  }

  @Override
  public void onAfterDetachPolicy(CatalogPolicyServiceEvents.AfterDetachPolicyEvent event) {
    recordEvent(event);
  }

  @Override
  public void onBeforeGetApplicablePolicies(
      CatalogPolicyServiceEvents.BeforeGetApplicablePoliciesEvent event) {
    recordEvent(event);
  }

  @Override
  public void onAfterGetApplicablePolicies(
      CatalogPolicyServiceEvents.AfterGetApplicablePoliciesEvent event) {
    recordEvent(event);
  }

  @Override
  public void onBeforeCreateGenericTable(
      CatalogGenericTableServiceEvents.BeforeCreateGenericTableEvent event) {
    recordEvent(event);
  }

  @Override
  public void onAfterCreateGenericTable(
      CatalogGenericTableServiceEvents.AfterCreateGenericTableEvent event) {
    recordEvent(event);
  }

  @Override
  public void onBeforeDropGenericTable(
      CatalogGenericTableServiceEvents.BeforeDropGenericTableEvent event) {
    recordEvent(event);
  }

  @Override
  public void onAfterDropGenericTable(
      CatalogGenericTableServiceEvents.AfterDropGenericTableEvent event) {
    recordEvent(event);
  }

  @Override
  public void onBeforeListGenericTables(
      CatalogGenericTableServiceEvents.BeforeListGenericTablesEvent event) {
    recordEvent(event);
  }

  @Override
  public void onAfterListGenericTables(
      CatalogGenericTableServiceEvents.AfterListGenericTablesEvent event) {
    recordEvent(event);
  }

  @Override
  public void onBeforeLoadGenericTable(
      CatalogGenericTableServiceEvents.BeforeLoadGenericTableEvent event) {
    recordEvent(event);
  }

  @Override
  public void onAfterLoadGenericTable(
      CatalogGenericTableServiceEvents.AfterLoadGenericTableEvent event) {
    recordEvent(event);
  }

  @Override
  public void onBeforeAttemptTask(BeforeAttemptTaskEvent event) {
    recordEvent(event);
  }

  @Override
  public void onAfterAttemptTask(AfterAttemptTaskEvent event) {
    recordEvent(event);
  }

  @Override
  public void onBeforeLimitRequestRate(BeforeLimitRequestRateEvent event) {
    recordEvent(event);
  }

  @Override
  public void onBeforeReportMetrics(IcebergRestCatalogEvents.BeforeReportMetricsEvent event) {
    recordEvent(event);
  }

  @Override
  public void onAfterReportMetrics(IcebergRestCatalogEvents.AfterReportMetricsEvent event) {
    recordEvent(event);
  }
}
