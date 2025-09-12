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
