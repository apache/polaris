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
public interface PolarisEventListener {
  /** {@link BeforeLimitRequestRateEvent} */
  default void onBeforeLimitRequestRate(BeforeLimitRequestRateEvent event) {}

  /** {@link IcebergRestCatalogEvents.BeforeCommitTableEvent} */
  default void onBeforeCommitTable(IcebergRestCatalogEvents.BeforeCommitTableEvent event) {}

  /** {@link IcebergRestCatalogEvents.AfterCommitTableEvent} */
  default void onAfterCommitTable(IcebergRestCatalogEvents.AfterCommitTableEvent event) {}

  /** {@link IcebergRestCatalogEvents.BeforeCommitViewEvent} */
  default void onBeforeCommitView(IcebergRestCatalogEvents.BeforeCommitViewEvent event) {}

  /** {@link IcebergRestCatalogEvents.AfterCommitViewEvent} */
  default void onAfterCommitView(IcebergRestCatalogEvents.AfterCommitViewEvent event) {}

  /** {@link IcebergRestCatalogEvents.BeforeRefreshTableEvent} */
  default void onBeforeRefreshTable(IcebergRestCatalogEvents.BeforeRefreshTableEvent event) {}

  /** {@link IcebergRestCatalogEvents.AfterRefreshTableEvent} */
  default void onAfterRefreshTable(IcebergRestCatalogEvents.AfterRefreshTableEvent event) {}

  /** {@link IcebergRestCatalogEvents.BeforeRefreshViewEvent} */
  default void onBeforeRefreshView(IcebergRestCatalogEvents.BeforeRefreshViewEvent event) {}

  /** {@link IcebergRestCatalogEvents.AfterRefreshViewEvent} */
  default void onAfterRefreshView(IcebergRestCatalogEvents.AfterRefreshViewEvent event) {}

  /** {@link BeforeAttemptTaskEvent} */
  default void onBeforeAttemptTask(BeforeAttemptTaskEvent event) {}

  /** {@link AfterAttemptTaskEvent} */
  default void onAfterAttemptTask(AfterAttemptTaskEvent event) {}

  // Iceberg REST Catalog Namespace Events
  /** {@link IcebergRestCatalogEvents.BeforeCreateNamespaceEvent} */
  default void onBeforeCreateNamespace(IcebergRestCatalogEvents.BeforeCreateNamespaceEvent event) {}

  /** {@link IcebergRestCatalogEvents.AfterCreateNamespaceEvent} */
  default void onAfterCreateNamespace(IcebergRestCatalogEvents.AfterCreateNamespaceEvent event) {}

  /** {@link IcebergRestCatalogEvents.BeforeListNamespacesEvent} */
  default void onBeforeListNamespaces(IcebergRestCatalogEvents.BeforeListNamespacesEvent event) {}

  /** {@link IcebergRestCatalogEvents.AfterListNamespacesEvent} */
  default void onAfterListNamespaces(IcebergRestCatalogEvents.AfterListNamespacesEvent event) {}

  /** {@link IcebergRestCatalogEvents.BeforeLoadNamespaceMetadataEvent} */
  default void onBeforeLoadNamespaceMetadata(
      IcebergRestCatalogEvents.BeforeLoadNamespaceMetadataEvent event) {}

  /** {@link IcebergRestCatalogEvents.AfterLoadNamespaceMetadataEvent} */
  default void onAfterLoadNamespaceMetadata(
      IcebergRestCatalogEvents.AfterLoadNamespaceMetadataEvent event) {}

  /** {@link IcebergRestCatalogEvents.BeforeCheckExistsNamespaceEvent} */
  default void onBeforeCheckExistsNamespace(
      IcebergRestCatalogEvents.BeforeCheckExistsNamespaceEvent event) {}

  /** {@link IcebergRestCatalogEvents.AfterCheckExistsNamespaceEvent} */
  default void onAfterCheckExistsNamespace(
      IcebergRestCatalogEvents.AfterCheckExistsNamespaceEvent event) {}

  /** {@link IcebergRestCatalogEvents.BeforeDropNamespaceEvent} */
  default void onBeforeDropNamespace(IcebergRestCatalogEvents.BeforeDropNamespaceEvent event) {}

  /** {@link IcebergRestCatalogEvents.AfterDropNamespaceEvent} */
  default void onAfterDropNamespace(IcebergRestCatalogEvents.AfterDropNamespaceEvent event) {}

  /** {@link IcebergRestCatalogEvents.BeforeUpdateNamespacePropertiesEvent} */
  default void onBeforeUpdateNamespaceProperties(
      IcebergRestCatalogEvents.BeforeUpdateNamespacePropertiesEvent event) {}

  /** {@link IcebergRestCatalogEvents.AfterUpdateNamespacePropertiesEvent} */
  default void onAfterUpdateNamespaceProperties(
      IcebergRestCatalogEvents.AfterUpdateNamespacePropertiesEvent event) {}

  // Iceberg REST Catalog Table Events
  /** {@link IcebergRestCatalogEvents.BeforeCreateTableEvent} */
  default void onBeforeCreateTable(IcebergRestCatalogEvents.BeforeCreateTableEvent event) {}

  /** {@link IcebergRestCatalogEvents.AfterCreateTableEvent} */
  default void onAfterCreateTable(IcebergRestCatalogEvents.AfterCreateTableEvent event) {}

  /** {@link IcebergRestCatalogEvents.BeforeListTablesEvent} */
  default void onBeforeListTables(IcebergRestCatalogEvents.BeforeListTablesEvent event) {}

  /** {@link IcebergRestCatalogEvents.AfterListTablesEvent} */
  default void onAfterListTables(IcebergRestCatalogEvents.AfterListTablesEvent event) {}

  /** {@link IcebergRestCatalogEvents.BeforeLoadTableEvent} */
  default void onBeforeLoadTable(IcebergRestCatalogEvents.BeforeLoadTableEvent event) {}

  /** {@link IcebergRestCatalogEvents.AfterLoadTableEvent} */
  default void onAfterLoadTable(IcebergRestCatalogEvents.AfterLoadTableEvent event) {}

  /** {@link IcebergRestCatalogEvents.BeforeCheckExistsTableEvent} */
  default void onBeforeCheckExistsTable(
      IcebergRestCatalogEvents.BeforeCheckExistsTableEvent event) {}

  /** {@link IcebergRestCatalogEvents.AfterCheckExistsTableEvent} */
  default void onAfterCheckExistsTable(IcebergRestCatalogEvents.AfterCheckExistsTableEvent event) {}

  /** {@link IcebergRestCatalogEvents.BeforeDropTableEvent} */
  default void onBeforeDropTable(IcebergRestCatalogEvents.BeforeDropTableEvent event) {}

  /** {@link IcebergRestCatalogEvents.AfterDropTableEvent} */
  default void onAfterDropTable(IcebergRestCatalogEvents.AfterDropTableEvent event) {}

  /** {@link IcebergRestCatalogEvents.BeforeRegisterTableEvent} */
  default void onBeforeRegisterTable(IcebergRestCatalogEvents.BeforeRegisterTableEvent event) {}

  /** {@link IcebergRestCatalogEvents.AfterRegisterTableEvent} */
  default void onAfterRegisterTable(IcebergRestCatalogEvents.AfterRegisterTableEvent event) {}

  /** {@link IcebergRestCatalogEvents.BeforeRenameTableEvent} */
  default void onBeforeRenameTable(IcebergRestCatalogEvents.BeforeRenameTableEvent event) {}

  /** {@link IcebergRestCatalogEvents.AfterRenameTableEvent} */
  default void onAfterRenameTable(IcebergRestCatalogEvents.AfterRenameTableEvent event) {}

  /** {@link IcebergRestCatalogEvents.BeforeUpdateTableEvent} */
  default void onBeforeUpdateTable(IcebergRestCatalogEvents.BeforeUpdateTableEvent event) {}

  /** {@link IcebergRestCatalogEvents.AfterUpdateTableEvent} */
  default void onAfterUpdateTable(IcebergRestCatalogEvents.AfterUpdateTableEvent event) {}

  // Iceberg REST Catalog View Events
  /** {@link IcebergRestCatalogEvents.BeforeCreateViewEvent} */
  default void onBeforeCreateView(IcebergRestCatalogEvents.BeforeCreateViewEvent event) {}

  /** {@link IcebergRestCatalogEvents.AfterCreateViewEvent} */
  default void onAfterCreateView(IcebergRestCatalogEvents.AfterCreateViewEvent event) {}

  /** {@link IcebergRestCatalogEvents.BeforeListViewsEvent} */
  default void onBeforeListViews(IcebergRestCatalogEvents.BeforeListViewsEvent event) {}

  /** {@link IcebergRestCatalogEvents.AfterListViewsEvent} */
  default void onAfterListViews(IcebergRestCatalogEvents.AfterListViewsEvent event) {}

  /** {@link IcebergRestCatalogEvents.BeforeLoadViewEvent} */
  default void onBeforeLoadView(IcebergRestCatalogEvents.BeforeLoadViewEvent event) {}

  /** {@link IcebergRestCatalogEvents.AfterLoadViewEvent} */
  default void onAfterLoadView(IcebergRestCatalogEvents.AfterLoadViewEvent event) {}

  /** {@link IcebergRestCatalogEvents.BeforeCheckExistsViewEvent} */
  default void onBeforeCheckExistsView(IcebergRestCatalogEvents.BeforeCheckExistsViewEvent event) {}

  /** {@link IcebergRestCatalogEvents.AfterCheckExistsViewEvent} */
  default void onAfterCheckExistsView(IcebergRestCatalogEvents.AfterCheckExistsViewEvent event) {}

  /** {@link IcebergRestCatalogEvents.BeforeDropViewEvent} */
  default void onBeforeDropView(IcebergRestCatalogEvents.BeforeDropViewEvent event) {}

  /** {@link IcebergRestCatalogEvents.AfterDropViewEvent} */
  default void onAfterDropView(IcebergRestCatalogEvents.AfterDropViewEvent event) {}

  /** {@link IcebergRestCatalogEvents.BeforeRenameViewEvent} */
  default void onBeforeRenameView(IcebergRestCatalogEvents.BeforeRenameViewEvent event) {}

  /** {@link IcebergRestCatalogEvents.AfterRenameViewEvent} */
  default void onAfterRenameView(IcebergRestCatalogEvents.AfterRenameViewEvent event) {}

  /** {@link IcebergRestCatalogEvents.BeforeReplaceViewEvent} */
  default void onBeforeReplaceView(IcebergRestCatalogEvents.BeforeReplaceViewEvent event) {}

  /** {@link IcebergRestCatalogEvents.AfterReplaceViewEvent} */
  default void onAfterReplaceView(IcebergRestCatalogEvents.AfterReplaceViewEvent event) {}

  // Iceberg REST Catalog Credential Events
  /** {@link IcebergRestCatalogEvents.BeforeLoadCredentialsEvent} */
  default void onBeforeLoadCredentials(IcebergRestCatalogEvents.BeforeLoadCredentialsEvent event) {}

  /** {@link IcebergRestCatalogEvents.AfterLoadCredentialsEvent} */
  default void onAfterLoadCredentials(IcebergRestCatalogEvents.AfterLoadCredentialsEvent event) {}

  // Iceberg REST Catalog Transactions Events
  /** {@link IcebergRestCatalogEvents.BeforeCommitTransactionEvent} */
  default void onBeforeCommitTransaction(
      IcebergRestCatalogEvents.BeforeCommitTransactionEvent event) {}

  /** {@link IcebergRestCatalogEvents.AfterCommitTransactionEvent} */
  default void onAfterCommitTransaction(
      IcebergRestCatalogEvents.AfterCommitTransactionEvent event) {}

  // Iceberg REST Catalog Notification Events
  /** {@link IcebergRestCatalogEvents.BeforeSendNotificationEvent} */
  default void onBeforeSendNotification(
      IcebergRestCatalogEvents.BeforeSendNotificationEvent event) {}

  /** {@link IcebergRestCatalogEvents.AfterSendNotificationEvent} */
  default void onAfterSendNotification(IcebergRestCatalogEvents.AfterSendNotificationEvent event) {}

  // Iceberg REST Catalog Configuration Events
  /** {@link IcebergRestCatalogEvents.BeforeGetConfigEvent} */
  default void onBeforeGetConfig(IcebergRestCatalogEvents.BeforeGetConfigEvent event) {}

  /** {@link IcebergRestCatalogEvents.AfterGetConfigEvent} */
  default void onAfterGetConfig(IcebergRestCatalogEvents.AfterGetConfigEvent event) {}

  // Catalog Policy Service Events
  /** {@link CatalogPolicyServiceEvents.BeforeCreatePolicyEvent} */
  default void onBeforeCreatePolicy(CatalogPolicyServiceEvents.BeforeCreatePolicyEvent event) {}

  /** {@link CatalogPolicyServiceEvents.AfterCreatePolicyEvent} */
  default void onAfterCreatePolicy(CatalogPolicyServiceEvents.AfterCreatePolicyEvent event) {}

  /** {@link CatalogPolicyServiceEvents.BeforeListPoliciesEvent} */
  default void onBeforeListPolicies(CatalogPolicyServiceEvents.BeforeListPoliciesEvent event) {}

  /** {@link CatalogPolicyServiceEvents.AfterListPoliciesEvent} */
  default void onAfterListPolicies(CatalogPolicyServiceEvents.AfterListPoliciesEvent event) {}

  /** {@link CatalogPolicyServiceEvents.BeforeLoadPolicyEvent} */
  default void onBeforeLoadPolicy(CatalogPolicyServiceEvents.BeforeLoadPolicyEvent event) {}

  /** {@link CatalogPolicyServiceEvents.AfterLoadPolicyEvent} */
  default void onAfterLoadPolicy(CatalogPolicyServiceEvents.AfterLoadPolicyEvent event) {}

  /** {@link CatalogPolicyServiceEvents.BeforeUpdatePolicyEvent} */
  default void onBeforeUpdatePolicy(CatalogPolicyServiceEvents.BeforeUpdatePolicyEvent event) {}

  /** {@link CatalogPolicyServiceEvents.AfterUpdatePolicyEvent} */
  default void onAfterUpdatePolicy(CatalogPolicyServiceEvents.AfterUpdatePolicyEvent event) {}

  /** {@link CatalogPolicyServiceEvents.BeforeDropPolicyEvent} */
  default void onBeforeDropPolicy(CatalogPolicyServiceEvents.BeforeDropPolicyEvent event) {}

  /** {@link CatalogPolicyServiceEvents.AfterDropPolicyEvent} */
  default void onAfterDropPolicy(CatalogPolicyServiceEvents.AfterDropPolicyEvent event) {}

  /** {@link CatalogPolicyServiceEvents.BeforeAttachPolicyEvent} */
  default void onBeforeAttachPolicy(CatalogPolicyServiceEvents.BeforeAttachPolicyEvent event) {}

  /** {@link CatalogPolicyServiceEvents.AfterAttachPolicyEvent} */
  default void onAfterAttachPolicy(CatalogPolicyServiceEvents.AfterAttachPolicyEvent event) {}

  /** {@link CatalogPolicyServiceEvents.BeforeDetachPolicyEvent} */
  default void onBeforeDetachPolicy(CatalogPolicyServiceEvents.BeforeDetachPolicyEvent event) {}

  /** {@link CatalogPolicyServiceEvents.AfterDetachPolicyEvent} */
  default void onAfterDetachPolicy(CatalogPolicyServiceEvents.AfterDetachPolicyEvent event) {}

  /** {@link CatalogPolicyServiceEvents.BeforeGetApplicablePoliciesEvent} */
  default void onBeforeGetApplicablePolicies(
      CatalogPolicyServiceEvents.BeforeGetApplicablePoliciesEvent event) {}

  /** {@link CatalogPolicyServiceEvents.AfterGetApplicablePoliciesEvent} */
  default void onAfterGetApplicablePolicies(
      CatalogPolicyServiceEvents.AfterGetApplicablePoliciesEvent event) {}

  /** {@link CatalogGenericTableServiceEvents.BeforeCreateGenericTableEvent} */
  default void onBeforeCreateGenericTable(
      CatalogGenericTableServiceEvents.BeforeCreateGenericTableEvent event) {}

  /** {@link CatalogGenericTableServiceEvents.AfterCreateGenericTableEvent} */
  default void onAfterCreateGenericTable(
      CatalogGenericTableServiceEvents.AfterCreateGenericTableEvent event) {}

  /** {@link CatalogGenericTableServiceEvents.BeforeDropGenericTableEvent} */
  default void onBeforeDropGenericTable(
      CatalogGenericTableServiceEvents.BeforeDropGenericTableEvent event) {}

  /** {@link CatalogGenericTableServiceEvents.AfterDropGenericTableEvent} */
  default void onAfterDropGenericTable(
      CatalogGenericTableServiceEvents.AfterDropGenericTableEvent event) {}

  /** {@link CatalogGenericTableServiceEvents.BeforeListGenericTablesEvent} */
  default void onBeforeListGenericTables(
      CatalogGenericTableServiceEvents.BeforeListGenericTablesEvent event) {}

  /** {@link CatalogGenericTableServiceEvents.AfterListGenericTablesEvent} */
  default void onAfterListGenericTables(
      CatalogGenericTableServiceEvents.AfterListGenericTablesEvent event) {}

  /** {@link CatalogGenericTableServiceEvents.BeforeLoadGenericTableEvent} */
  default void onBeforeLoadGenericTable(
      CatalogGenericTableServiceEvents.BeforeLoadGenericTableEvent event) {}

  /** {@link CatalogGenericTableServiceEvents.AfterLoadGenericTableEvent} */
  default void onAfterLoadGenericTable(
      CatalogGenericTableServiceEvents.AfterLoadGenericTableEvent event) {}
}
