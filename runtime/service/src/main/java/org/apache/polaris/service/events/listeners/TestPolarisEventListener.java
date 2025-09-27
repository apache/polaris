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

import com.google.common.collect.Streams;
import io.smallrye.common.annotation.Identifier;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.ArrayList;
import java.util.List;
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

/** Event listener that stores all emitted events forever. Not recommended for use in production. */
@ApplicationScoped
@Identifier("test")
public class TestPolarisEventListener implements PolarisEventListener {
  private final List<PolarisEvent> history = new ArrayList<>();

  public <T> T getLatest(Class<T> type) {
    return Streams.findLast(history.stream().filter(type::isInstance))
        .map(type::cast)
        .orElseThrow();
  }

  @Override
  public void onBeforeCreateCatalog(CatalogsServiceEvents.BeforeCreateCatalogEvent event) {
    history.add(event);
  }

  @Override
  public void onAfterCreateCatalog(CatalogsServiceEvents.AfterCreateCatalogEvent event) {
    history.add(event);
  }

  @Override
  public void onBeforeDeleteCatalog(CatalogsServiceEvents.BeforeDeleteCatalogEvent event) {
    history.add(event);
  }

  @Override
  public void onAfterDeleteCatalog(CatalogsServiceEvents.AfterDeleteCatalogEvent event) {
    history.add(event);
  }

  @Override
  public void onBeforeGetCatalog(CatalogsServiceEvents.BeforeGetCatalogEvent event) {
    history.add(event);
  }

  @Override
  public void onAfterGetCatalog(CatalogsServiceEvents.AfterGetCatalogEvent event) {
    history.add(event);
  }

  @Override
  public void onBeforeUpdateCatalog(CatalogsServiceEvents.BeforeUpdateCatalogEvent event) {
    history.add(event);
  }

  @Override
  public void onAfterUpdateCatalog(CatalogsServiceEvents.AfterUpdateCatalogEvent event) {
    history.add(event);
  }

  @Override
  public void onBeforeListCatalogs(CatalogsServiceEvents.BeforeListCatalogsEvent event) {
    history.add(event);
  }

  @Override
  public void onAfterListCatalogs(CatalogsServiceEvents.AfterListCatalogsEvent event) {
    history.add(event);
  }

  @Override
  public void onBeforeCreatePrincipal(PrincipalsServiceEvents.BeforeCreatePrincipalEvent event) {
    history.add(event);
  }

  @Override
  public void onAfterCreatePrincipal(PrincipalsServiceEvents.AfterCreatePrincipalEvent event) {
    history.add(event);
  }

  @Override
  public void onBeforeDeletePrincipal(PrincipalsServiceEvents.BeforeDeletePrincipalEvent event) {
    history.add(event);
  }

  @Override
  public void onAfterDeletePrincipal(PrincipalsServiceEvents.AfterDeletePrincipalEvent event) {
    history.add(event);
  }

  @Override
  public void onBeforeGetPrincipal(PrincipalsServiceEvents.BeforeGetPrincipalEvent event) {
    history.add(event);
  }

  @Override
  public void onAfterGetPrincipal(PrincipalsServiceEvents.AfterGetPrincipalEvent event) {
    history.add(event);
  }

  @Override
  public void onBeforeUpdatePrincipal(PrincipalsServiceEvents.BeforeUpdatePrincipalEvent event) {
    history.add(event);
  }

  @Override
  public void onAfterUpdatePrincipal(PrincipalsServiceEvents.AfterUpdatePrincipalEvent event) {
    history.add(event);
  }

  @Override
  public void onBeforeRotateCredentials(
      PrincipalsServiceEvents.BeforeRotateCredentialsEvent event) {
    history.add(event);
  }

  @Override
  public void onAfterRotateCredentials(PrincipalsServiceEvents.AfterRotateCredentialsEvent event) {
    history.add(event);
  }

  @Override
  public void onBeforeListPrincipals(PrincipalsServiceEvents.BeforeListPrincipalsEvent event) {
    history.add(event);
  }

  @Override
  public void onAfterListPrincipals(PrincipalsServiceEvents.AfterListPrincipalsEvent event) {
    history.add(event);
  }

  @Override
  public void onBeforeResetCredentials(PrincipalsServiceEvents.BeforeResetCredentialsEvent event) {
    history.add(event);
  }

  @Override
  public void onAfterResetCredentials(PrincipalsServiceEvents.AfterResetCredentialsEvent event) {
    history.add(event);
  }

  @Override
  public void onBeforeAssignPrincipalRole(
      PrincipalsServiceEvents.BeforeAssignPrincipalRoleEvent event) {
    history.add(event);
  }

  @Override
  public void onAfterAssignPrincipalRole(
      PrincipalsServiceEvents.AfterAssignPrincipalRoleEvent event) {
    history.add(event);
  }

  @Override
  public void onBeforeRevokePrincipalRole(
      PrincipalsServiceEvents.BeforeRevokePrincipalRoleEvent event) {
    history.add(event);
  }

  @Override
  public void onAfterRevokePrincipalRole(
      PrincipalsServiceEvents.AfterRevokePrincipalRoleEvent event) {
    history.add(event);
  }

  @Override
  public void onBeforeListAssignedPrincipalRoles(
      PrincipalsServiceEvents.BeforeListAssignedPrincipalRolesEvent event) {
    history.add(event);
  }

  @Override
  public void onAfterListAssignedPrincipalRoles(
      PrincipalsServiceEvents.AfterListAssignedPrincipalRolesEvent event) {
    history.add(event);
  }

  @Override
  public void onBeforeCreatePrincipalRole(
      PrincipalRolesServiceEvents.BeforeCreatePrincipalRoleEvent event) {
    history.add(event);
  }

  @Override
  public void onAfterCreatePrincipalRole(
      PrincipalRolesServiceEvents.AfterCreatePrincipalRoleEvent event) {
    history.add(event);
  }

  @Override
  public void onBeforeDeletePrincipalRole(
      PrincipalRolesServiceEvents.BeforeDeletePrincipalRoleEvent event) {
    history.add(event);
  }

  @Override
  public void onAfterDeletePrincipalRole(
      PrincipalRolesServiceEvents.AfterDeletePrincipalRoleEvent event) {
    history.add(event);
  }

  @Override
  public void onBeforeGetPrincipalRole(
      PrincipalRolesServiceEvents.BeforeGetPrincipalRoleEvent event) {
    history.add(event);
  }

  @Override
  public void onAfterGetPrincipalRole(
      PrincipalRolesServiceEvents.AfterGetPrincipalRoleEvent event) {
    history.add(event);
  }

  @Override
  public void onBeforeUpdatePrincipalRole(
      PrincipalRolesServiceEvents.BeforeUpdatePrincipalRoleEvent event) {
    history.add(event);
  }

  @Override
  public void onAfterUpdatePrincipalRole(
      PrincipalRolesServiceEvents.AfterUpdatePrincipalRoleEvent event) {
    history.add(event);
  }

  @Override
  public void onBeforeListPrincipalRoles(
      PrincipalRolesServiceEvents.BeforeListPrincipalRolesEvent event) {
    history.add(event);
  }

  @Override
  public void onAfterListPrincipalRoles(
      PrincipalRolesServiceEvents.AfterListPrincipalRolesEvent event) {
    history.add(event);
  }

  @Override
  public void onBeforeCreateCatalogRole(CatalogsServiceEvents.BeforeCreateCatalogRoleEvent event) {
    history.add(event);
  }

  @Override
  public void onAfterCreateCatalogRole(CatalogsServiceEvents.AfterCreateCatalogRoleEvent event) {
    history.add(event);
  }

  @Override
  public void onBeforeDeleteCatalogRole(CatalogsServiceEvents.BeforeDeleteCatalogRoleEvent event) {
    history.add(event);
  }

  @Override
  public void onAfterDeleteCatalogRole(CatalogsServiceEvents.AfterDeleteCatalogRoleEvent event) {
    history.add(event);
  }

  @Override
  public void onBeforeGetCatalogRole(CatalogsServiceEvents.BeforeGetCatalogRoleEvent event) {
    history.add(event);
  }

  @Override
  public void onAfterGetCatalogRole(CatalogsServiceEvents.AfterGetCatalogRoleEvent event) {
    history.add(event);
  }

  @Override
  public void onBeforeUpdateCatalogRole(CatalogsServiceEvents.BeforeUpdateCatalogRoleEvent event) {
    history.add(event);
  }

  @Override
  public void onAfterUpdateCatalogRole(CatalogsServiceEvents.AfterUpdateCatalogRoleEvent event) {
    history.add(event);
  }

  @Override
  public void onBeforeListCatalogRoles(CatalogsServiceEvents.BeforeListCatalogRolesEvent event) {
    history.add(event);
  }

  @Override
  public void onAfterListCatalogRoles(CatalogsServiceEvents.AfterListCatalogRolesEvent event) {
    history.add(event);
  }

  @Override
  public void onBeforeAssignCatalogRoleToPrincipalRole(
      PrincipalRolesServiceEvents.BeforeAssignCatalogRoleToPrincipalRoleEvent event) {
    history.add(event);
  }

  @Override
  public void onAfterAssignCatalogRoleToPrincipalRole(
      PrincipalRolesServiceEvents.AfterAssignCatalogRoleToPrincipalRoleEvent event) {
    history.add(event);
  }

  @Override
  public void onBeforeRevokeCatalogRoleFromPrincipalRole(
      PrincipalRolesServiceEvents.BeforeRevokeCatalogRoleFromPrincipalRoleEvent event) {
    history.add(event);
  }

  @Override
  public void onAfterRevokeCatalogRoleFromPrincipalRole(
      PrincipalRolesServiceEvents.AfterRevokeCatalogRoleFromPrincipalRoleEvent event) {
    history.add(event);
  }

  @Override
  public void onBeforeListAssigneePrincipalsForPrincipalRole(
      PrincipalRolesServiceEvents.BeforeListAssigneePrincipalsForPrincipalRoleEvent event) {
    history.add(event);
  }

  @Override
  public void onAfterListAssigneePrincipalsForPrincipalRole(
      PrincipalRolesServiceEvents.AfterListAssigneePrincipalsForPrincipalRoleEvent event) {
    history.add(event);
  }

  @Override
  public void onBeforeListCatalogRolesForPrincipalRole(
      PrincipalRolesServiceEvents.BeforeListCatalogRolesForPrincipalRoleEvent event) {
    history.add(event);
  }

  @Override
  public void onAfterListCatalogRolesForPrincipalRole(
      PrincipalRolesServiceEvents.AfterListCatalogRolesForPrincipalRoleEvent event) {
    history.add(event);
  }

  @Override
  public void onBeforeAddGrantToCatalogRole(
      CatalogsServiceEvents.BeforeAddGrantToCatalogRoleEvent event) {
    history.add(event);
  }

  @Override
  public void onAfterAddGrantToCatalogRole(
      CatalogsServiceEvents.AfterAddGrantToCatalogRoleEvent event) {
    history.add(event);
  }

  @Override
  public void onBeforeRevokeGrantFromCatalogRole(
      CatalogsServiceEvents.BeforeRevokeGrantFromCatalogRoleEvent event) {
    history.add(event);
  }

  @Override
  public void onAfterRevokeGrantFromCatalogRole(
      CatalogsServiceEvents.AfterRevokeGrantFromCatalogRoleEvent event) {
    history.add(event);
  }

  @Override
  public void onBeforeListAssigneePrincipalRolesForCatalogRole(
      CatalogsServiceEvents.BeforeListAssigneePrincipalRolesForCatalogRoleEvent event) {
    history.add(event);
  }

  @Override
  public void onAfterListAssigneePrincipalRolesForCatalogRole(
      CatalogsServiceEvents.AfterListAssigneePrincipalRolesForCatalogRoleEvent event) {
    history.add(event);
  }

  @Override
  public void onBeforeListGrantsForCatalogRole(
      CatalogsServiceEvents.BeforeListGrantsForCatalogRoleEvent event) {
    history.add(event);
  }

  @Override
  public void onAfterListGrantsForCatalogRole(
      CatalogsServiceEvents.AfterListGrantsForCatalogRoleEvent event) {
    history.add(event);
  }

  @Override
  public void onBeforeCreateNamespace(IcebergRestCatalogEvents.BeforeCreateNamespaceEvent event) {
    history.add(event);
  }

  @Override
  public void onAfterCreateNamespace(IcebergRestCatalogEvents.AfterCreateNamespaceEvent event) {
    history.add(event);
  }

  @Override
  public void onBeforeListNamespaces(IcebergRestCatalogEvents.BeforeListNamespacesEvent event) {
    history.add(event);
  }

  @Override
  public void onAfterListNamespaces(IcebergRestCatalogEvents.AfterListNamespacesEvent event) {
    history.add(event);
  }

  @Override
  public void onBeforeLoadNamespaceMetadata(
      IcebergRestCatalogEvents.BeforeLoadNamespaceMetadataEvent event) {
    history.add(event);
  }

  @Override
  public void onAfterLoadNamespaceMetadata(
      IcebergRestCatalogEvents.AfterLoadNamespaceMetadataEvent event) {
    history.add(event);
  }

  @Override
  public void onBeforeCheckExistsNamespace(
      IcebergRestCatalogEvents.BeforeCheckExistsNamespaceEvent event) {
    history.add(event);
  }

  @Override
  public void onAfterCheckExistsNamespace(
      IcebergRestCatalogEvents.AfterCheckExistsNamespaceEvent event) {
    history.add(event);
  }

  @Override
  public void onBeforeDropNamespace(IcebergRestCatalogEvents.BeforeDropNamespaceEvent event) {
    history.add(event);
  }

  @Override
  public void onAfterDropNamespace(IcebergRestCatalogEvents.AfterDropNamespaceEvent event) {
    history.add(event);
  }

  @Override
  public void onBeforeUpdateNamespaceProperties(
      IcebergRestCatalogEvents.BeforeUpdateNamespacePropertiesEvent event) {
    history.add(event);
  }

  @Override
  public void onAfterUpdateNamespaceProperties(
      IcebergRestCatalogEvents.AfterUpdateNamespacePropertiesEvent event) {
    history.add(event);
  }

  @Override
  public void onBeforeCreateTable(IcebergRestCatalogEvents.BeforeCreateTableEvent event) {
    history.add(event);
  }

  @Override
  public void onAfterCreateTable(IcebergRestCatalogEvents.AfterCreateTableEvent event) {
    history.add(event);
  }

  @Override
  public void onBeforeCommitTable(IcebergRestCatalogEvents.BeforeCommitTableEvent event) {
    history.add(event);
  }

  @Override
  public void onAfterCommitTable(IcebergRestCatalogEvents.AfterCommitTableEvent event) {
    history.add(event);
  }

  @Override
  public void onBeforeRefreshTable(IcebergRestCatalogEvents.BeforeRefreshTableEvent event) {
    history.add(event);
  }

  @Override
  public void onAfterRefreshTable(IcebergRestCatalogEvents.AfterRefreshTableEvent event) {
    history.add(event);
  }

  @Override
  public void onBeforeListTables(IcebergRestCatalogEvents.BeforeListTablesEvent event) {
    history.add(event);
  }

  @Override
  public void onAfterListTables(IcebergRestCatalogEvents.AfterListTablesEvent event) {
    history.add(event);
  }

  @Override
  public void onBeforeLoadTable(IcebergRestCatalogEvents.BeforeLoadTableEvent event) {
    history.add(event);
  }

  @Override
  public void onAfterLoadTable(IcebergRestCatalogEvents.AfterLoadTableEvent event) {
    history.add(event);
  }

  @Override
  public void onBeforeCheckExistsTable(IcebergRestCatalogEvents.BeforeCheckExistsTableEvent event) {
    history.add(event);
  }

  @Override
  public void onAfterCheckExistsTable(IcebergRestCatalogEvents.AfterCheckExistsTableEvent event) {
    history.add(event);
  }

  @Override
  public void onBeforeDropTable(IcebergRestCatalogEvents.BeforeDropTableEvent event) {
    history.add(event);
  }

  @Override
  public void onAfterDropTable(IcebergRestCatalogEvents.AfterDropTableEvent event) {
    history.add(event);
  }

  @Override
  public void onBeforeRegisterTable(IcebergRestCatalogEvents.BeforeRegisterTableEvent event) {
    history.add(event);
  }

  @Override
  public void onAfterRegisterTable(IcebergRestCatalogEvents.AfterRegisterTableEvent event) {
    history.add(event);
  }

  @Override
  public void onBeforeRenameTable(IcebergRestCatalogEvents.BeforeRenameTableEvent event) {
    history.add(event);
  }

  @Override
  public void onAfterRenameTable(IcebergRestCatalogEvents.AfterRenameTableEvent event) {
    history.add(event);
  }

  @Override
  public void onBeforeUpdateTable(IcebergRestCatalogEvents.BeforeUpdateTableEvent event) {
    history.add(event);
  }

  @Override
  public void onAfterUpdateTable(IcebergRestCatalogEvents.AfterUpdateTableEvent event) {
    history.add(event);
  }

  @Override
  public void onBeforeCreateView(IcebergRestCatalogEvents.BeforeCreateViewEvent event) {
    history.add(event);
  }

  @Override
  public void onAfterCreateView(IcebergRestCatalogEvents.AfterCreateViewEvent event) {
    history.add(event);
  }

  @Override
  public void onBeforeCommitView(IcebergRestCatalogEvents.BeforeCommitViewEvent event) {
    history.add(event);
  }

  @Override
  public void onAfterCommitView(IcebergRestCatalogEvents.AfterCommitViewEvent event) {
    history.add(event);
  }

  @Override
  public void onBeforeRefreshView(IcebergRestCatalogEvents.BeforeRefreshViewEvent event) {
    history.add(event);
  }

  @Override
  public void onAfterRefreshView(IcebergRestCatalogEvents.AfterRefreshViewEvent event) {
    history.add(event);
  }

  @Override
  public void onBeforeListViews(IcebergRestCatalogEvents.BeforeListViewsEvent event) {
    history.add(event);
  }

  @Override
  public void onAfterListViews(IcebergRestCatalogEvents.AfterListViewsEvent event) {
    history.add(event);
  }

  @Override
  public void onBeforeLoadView(IcebergRestCatalogEvents.BeforeLoadViewEvent event) {
    history.add(event);
  }

  @Override
  public void onAfterLoadView(IcebergRestCatalogEvents.AfterLoadViewEvent event) {
    history.add(event);
  }

  @Override
  public void onBeforeCheckExistsView(IcebergRestCatalogEvents.BeforeCheckExistsViewEvent event) {
    history.add(event);
  }

  @Override
  public void onAfterCheckExistsView(IcebergRestCatalogEvents.AfterCheckExistsViewEvent event) {
    history.add(event);
  }

  @Override
  public void onBeforeDropView(IcebergRestCatalogEvents.BeforeDropViewEvent event) {
    history.add(event);
  }

  @Override
  public void onAfterDropView(IcebergRestCatalogEvents.AfterDropViewEvent event) {
    history.add(event);
  }

  @Override
  public void onBeforeRenameView(IcebergRestCatalogEvents.BeforeRenameViewEvent event) {
    history.add(event);
  }

  @Override
  public void onAfterRenameView(IcebergRestCatalogEvents.AfterRenameViewEvent event) {
    history.add(event);
  }

  @Override
  public void onBeforeReplaceView(IcebergRestCatalogEvents.BeforeReplaceViewEvent event) {
    history.add(event);
  }

  @Override
  public void onAfterReplaceView(IcebergRestCatalogEvents.AfterReplaceViewEvent event) {
    history.add(event);
  }

  @Override
  public void onBeforeLoadCredentials(IcebergRestCatalogEvents.BeforeLoadCredentialsEvent event) {
    history.add(event);
  }

  @Override
  public void onAfterLoadCredentials(IcebergRestCatalogEvents.AfterLoadCredentialsEvent event) {
    history.add(event);
  }

  @Override
  public void onBeforeCommitTransaction(
      IcebergRestCatalogEvents.BeforeCommitTransactionEvent event) {
    history.add(event);
  }

  @Override
  public void onAfterCommitTransaction(IcebergRestCatalogEvents.AfterCommitTransactionEvent event) {
    history.add(event);
  }

  @Override
  public void onBeforeSendNotification(IcebergRestCatalogEvents.BeforeSendNotificationEvent event) {
    history.add(event);
  }

  @Override
  public void onAfterSendNotification(IcebergRestCatalogEvents.AfterSendNotificationEvent event) {
    history.add(event);
  }

  @Override
  public void onBeforeGetConfig(IcebergRestCatalogEvents.BeforeGetConfigEvent event) {
    history.add(event);
  }

  @Override
  public void onAfterGetConfig(IcebergRestCatalogEvents.AfterGetConfigEvent event) {
    history.add(event);
  }

  @Override
  public void onBeforeCreatePolicy(CatalogPolicyServiceEvents.BeforeCreatePolicyEvent event) {
    history.add(event);
  }

  @Override
  public void onAfterCreatePolicy(CatalogPolicyServiceEvents.AfterCreatePolicyEvent event) {
    history.add(event);
  }

  @Override
  public void onBeforeListPolicies(CatalogPolicyServiceEvents.BeforeListPoliciesEvent event) {
    history.add(event);
  }

  @Override
  public void onAfterListPolicies(CatalogPolicyServiceEvents.AfterListPoliciesEvent event) {
    history.add(event);
  }

  @Override
  public void onBeforeLoadPolicy(CatalogPolicyServiceEvents.BeforeLoadPolicyEvent event) {
    history.add(event);
  }

  @Override
  public void onAfterLoadPolicy(CatalogPolicyServiceEvents.AfterLoadPolicyEvent event) {
    history.add(event);
  }

  @Override
  public void onBeforeUpdatePolicy(CatalogPolicyServiceEvents.BeforeUpdatePolicyEvent event) {
    history.add(event);
  }

  @Override
  public void onAfterUpdatePolicy(CatalogPolicyServiceEvents.AfterUpdatePolicyEvent event) {
    history.add(event);
  }

  @Override
  public void onBeforeDropPolicy(CatalogPolicyServiceEvents.BeforeDropPolicyEvent event) {
    history.add(event);
  }

  @Override
  public void onAfterDropPolicy(CatalogPolicyServiceEvents.AfterDropPolicyEvent event) {
    history.add(event);
  }

  @Override
  public void onBeforeAttachPolicy(CatalogPolicyServiceEvents.BeforeAttachPolicyEvent event) {
    history.add(event);
  }

  @Override
  public void onAfterAttachPolicy(CatalogPolicyServiceEvents.AfterAttachPolicyEvent event) {
    history.add(event);
  }

  @Override
  public void onBeforeDetachPolicy(CatalogPolicyServiceEvents.BeforeDetachPolicyEvent event) {
    history.add(event);
  }

  @Override
  public void onAfterDetachPolicy(CatalogPolicyServiceEvents.AfterDetachPolicyEvent event) {
    history.add(event);
  }

  @Override
  public void onBeforeGetApplicablePolicies(
      CatalogPolicyServiceEvents.BeforeGetApplicablePoliciesEvent event) {
    history.add(event);
  }

  @Override
  public void onAfterGetApplicablePolicies(
      CatalogPolicyServiceEvents.AfterGetApplicablePoliciesEvent event) {
    history.add(event);
  }

  @Override
  public void onBeforeCreateGenericTable(
      CatalogGenericTableServiceEvents.BeforeCreateGenericTableEvent event) {
    history.add(event);
  }

  @Override
  public void onAfterCreateGenericTable(
      CatalogGenericTableServiceEvents.AfterCreateGenericTableEvent event) {
    history.add(event);
  }

  @Override
  public void onBeforeDropGenericTable(
      CatalogGenericTableServiceEvents.BeforeDropGenericTableEvent event) {
    history.add(event);
  }

  @Override
  public void onAfterDropGenericTable(
      CatalogGenericTableServiceEvents.AfterDropGenericTableEvent event) {
    history.add(event);
  }

  @Override
  public void onBeforeListGenericTables(
      CatalogGenericTableServiceEvents.BeforeListGenericTablesEvent event) {
    history.add(event);
  }

  @Override
  public void onAfterListGenericTables(
      CatalogGenericTableServiceEvents.AfterListGenericTablesEvent event) {
    history.add(event);
  }

  @Override
  public void onBeforeLoadGenericTable(
      CatalogGenericTableServiceEvents.BeforeLoadGenericTableEvent event) {
    history.add(event);
  }

  @Override
  public void onAfterLoadGenericTable(
      CatalogGenericTableServiceEvents.AfterLoadGenericTableEvent event) {
    history.add(event);
  }

  @Override
  public void onBeforeAttemptTask(BeforeAttemptTaskEvent event) {
    history.add(event);
  }

  @Override
  public void onAfterAttemptTask(AfterAttemptTaskEvent event) {
    history.add(event);
  }

  @Override
  public void onBeforeLimitRequestRate(BeforeLimitRequestRateEvent event) {
    history.add(event);
  }
}
