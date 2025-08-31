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
  public void onBeforeLoadNamespaceMetadata(IcebergRestCatalogEvents.BeforeLoadNamespaceMetadataEvent event) {}

  /** {@link IcebergRestCatalogEvents.AfterLoadNamespaceMetadataEvent} */
  public void onAfterLoadNamespaceMetadata(IcebergRestCatalogEvents.AfterLoadNamespaceMetadataEvent event) {}

  /** {@link IcebergRestCatalogEvents.BeforeCheckNamespaceExistsEvent} */
  public void onBeforeCheckNamespaceExists(IcebergRestCatalogEvents.BeforeCheckNamespaceExistsEvent event) {}

  /** {@link IcebergRestCatalogEvents.AfterCheckNamespaceExistsEvent} */
  public void onAfterCheckNamespaceExists(IcebergRestCatalogEvents.AfterCheckNamespaceExistsEvent event) {}

  /** {@link IcebergRestCatalogEvents.BeforeDropNamespaceEvent} */
  public void onBeforeDropNamespace(IcebergRestCatalogEvents.BeforeDropNamespaceEvent event) {}

  /** {@link IcebergRestCatalogEvents.AfterDropNamespaceEvent} */
  public void onAfterDropNamespace(IcebergRestCatalogEvents.AfterDropNamespaceEvent event) {}

  /** {@link IcebergRestCatalogEvents.BeforeUpdateNamespacePropertiesEvent} */
  public void onBeforeUpdateNamespaceProperties(IcebergRestCatalogEvents.BeforeUpdateNamespacePropertiesEvent event) {}

  /** {@link IcebergRestCatalogEvents.AfterUpdateNamespacePropertiesEvent} */
  public void onAfterUpdateNamespaceProperties(IcebergRestCatalogEvents.AfterUpdateNamespacePropertiesEvent event) {}

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

  /** {@link IcebergRestCatalogEvents.BeforeCheckTableExistsEvent} */
  public void onBeforeCheckTableExists(IcebergRestCatalogEvents.BeforeCheckTableExistsEvent event) {}

  /** {@link IcebergRestCatalogEvents.AfterCheckTableExistsEvent} */
  public void onAfterCheckTableExists(IcebergRestCatalogEvents.AfterCheckTableExistsEvent event) {}

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

  /** {@link IcebergRestCatalogEvents.BeforeCheckViewExistsEvent} */
  public void onBeforeCheckViewExists(IcebergRestCatalogEvents.BeforeCheckViewExistsEvent event) {}

  /** {@link IcebergRestCatalogEvents.AfterCheckViewExistsEvent} */
  public void onAfterCheckViewExists(IcebergRestCatalogEvents.AfterCheckViewExistsEvent event) {}

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

  // Iceberg REST Catalog Notification Events
  /** {@link IcebergRestCatalogEvents.BeforeSendNotificationEvent} */
  public void onBeforeSendNotification(IcebergRestCatalogEvents.BeforeSendNotificationEvent event) {}

  /** {@link IcebergRestCatalogEvents.AfterSendNotificationEvent} */
  public void onAfterSendNotification(IcebergRestCatalogEvents.AfterSendNotificationEvent event) {}

  // Iceberg REST Catalog Configuration Events
  /** {@link IcebergRestCatalogEvents.BeforeGetConfigEvent} */
  public void onBeforeGetConfig(IcebergRestCatalogEvents.BeforeGetConfigEvent event) {}

  /** {@link IcebergRestCatalogEvents.AfterGetConfigEvent} */
  public void onAfterGetConfig(IcebergRestCatalogEvents.AfterGetConfigEvent event) {}
}
