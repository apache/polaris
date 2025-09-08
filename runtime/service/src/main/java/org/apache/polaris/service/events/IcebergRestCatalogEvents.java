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

import java.util.Map;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.requests.CreateNamespaceRequest;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.rest.requests.CreateViewRequest;
import org.apache.iceberg.rest.requests.RegisterTableRequest;
import org.apache.iceberg.rest.requests.RenameTableRequest;
import org.apache.iceberg.rest.requests.UpdateNamespacePropertiesRequest;
import org.apache.iceberg.rest.responses.ConfigResponse;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.iceberg.rest.responses.LoadViewResponse;
import org.apache.iceberg.rest.responses.UpdateNamespacePropertiesResponse;
import org.apache.iceberg.view.ViewMetadata;
import org.apache.polaris.service.types.CommitTableRequest;
import org.apache.polaris.service.types.CommitViewRequest;
import org.apache.polaris.service.types.NotificationRequest;

/**
 * Event records for Iceberg REST Catalog operations. Each operation has corresponding "Before" and
 * "After" event records.
 */
public class IcebergRestCatalogEvents {

  // Namespace Events
  public record BeforeCreateNamespaceEvent(
      String catalogName, CreateNamespaceRequest createNamespaceRequest) {}

  public record AfterCreateNamespaceEvent(
      String catalogName, Namespace namespace, Map<String, String> namespaceProperties) {}

  public record BeforeListNamespacesEvent(String catalogName, String parent) {}

  public record AfterListNamespacesEvent(String catalogName, String parent) {}

  public record BeforeLoadNamespaceMetadataEvent(String catalogName, Namespace namespace) {}

  public record AfterLoadNamespaceMetadataEvent(
      String catalogName, Namespace namespace, Map<String, String> namespaceProperties) {}

  public record BeforeCheckExistsNamespaceEvent(String catalogName, Namespace namespace) {}

  public record AfterCheckExistsNamespaceEvent(String catalogName, Namespace namespace) {}

  public record BeforeDropNamespaceEvent(String catalogName, Namespace namespace) {}

  public record AfterDropNamespaceEvent(String catalogName, String namespace) {}

  public record BeforeUpdateNamespacePropertiesEvent(
      String catalogName,
      Namespace namespace,
      UpdateNamespacePropertiesRequest updateNamespacePropertiesRequest) {}

  public record AfterUpdateNamespacePropertiesEvent(
      String catalogName,
      Namespace namespace,
      UpdateNamespacePropertiesResponse updateNamespacePropertiesResponse) {}

  // Table Events
  public record BeforeCreateTableEvent(
      String catalogName,
      Namespace namespace,
      CreateTableRequest createTableRequest,
      String accessDelegationMode) {}

  public record AfterCreateTableEvent(
      String catalogName,
      Namespace namespace,
      String tableName,
      LoadTableResponse loadTableResponse) {}

  public record BeforeListTablesEvent(String catalogName, Namespace namespace) {}

  public record AfterListTablesEvent(String catalogName, Namespace namespace) {}

  public record BeforeLoadTableEvent(
      String catalogName,
      Namespace namespace,
      String table,
      String accessDelegationMode,
      String ifNoneMatchString,
      String snapshots) {}

  public record AfterLoadTableEvent(
      String catalogName, Namespace namespace, LoadTableResponse loadTableResponse) {}

  public record BeforeCheckExistsTableEvent(
      String catalogName, Namespace namespace, String table) {}

  public record AfterCheckExistsTableEvent(String catalogName, Namespace namespace, String table) {}

  public record BeforeDropTableEvent(
      String catalogName, Namespace namespace, String table, Boolean purgeRequested) {}

  public record AfterDropTableEvent(
      String catalogName, Namespace namespace, String table, Boolean purgeRequested) {}

  public record BeforeRegisterTableEvent(
      String catalogName, Namespace namespace, RegisterTableRequest registerTableRequest) {}

  public record AfterRegisterTableEvent(
      String catalogName, Namespace namespace, LoadTableResponse loadTableResponse) {}

  public record BeforeRenameTableEvent(String catalogName, RenameTableRequest renameTableRequest) {}

  public record AfterRenameTableEvent(String catalogName, RenameTableRequest renameTableRequest) {}

  public record BeforeUpdateTableEvent(
      String catalogName,
      Namespace namespace,
      String sourceTable,
      CommitTableRequest commitTableRequest) {}

  public record AfterUpdateTableEvent(
      String catalogName,
      Namespace namespace,
      String sourceTable,
      LoadTableResponse loadTableResponse) {}

  // View Events
  public record BeforeCreateViewEvent(
      String catalogName, Namespace namespace, CreateViewRequest createViewRequest) {}

  public record AfterCreateViewEvent(
      String catalogName, Namespace namespace, LoadViewResponse loadViewResponse) {}

  public record BeforeListViewsEvent(String catalogName, Namespace namespace) {}

  public record AfterListViewsEvent(String catalogName, Namespace namespace) {}

  public record BeforeLoadViewEvent(String catalogName, Namespace namespace, String view) {}

  public record AfterLoadViewEvent(
      String catalogName, Namespace namespace, LoadViewResponse loadViewResponse) {}

  public record BeforeCheckExistsViewEvent(String catalogName, Namespace namespace, String view) {}

  public record AfterCheckExistsViewEvent(String catalogName, Namespace namespace, String view) {}

  public record BeforeDropViewEvent(String catalogName, Namespace namespace, String view) {}

  public record AfterDropViewEvent(String catalogName, Namespace namespace, String view) {}

  public record BeforeRenameViewEvent(String catalogName, RenameTableRequest renameTableRequest) {}

  public record AfterRenameViewEvent(String catalogName, RenameTableRequest renameTableRequest) {}

  public record BeforeReplaceViewEvent(
      String catalogName,
      Namespace namespace,
      String sourceView,
      CommitViewRequest commitViewRequest) {}

  public record AfterReplaceViewEvent(
      String catalogName,
      Namespace namespace,
      String sourceView,
      LoadViewResponse loadViewResponse) {}

  // Credential Events
  public record BeforeLoadCredentialsEvent(String catalogName, Namespace namespace, String table) {}

  public record AfterLoadCredentialsEvent(String catalogName, Namespace namespace, String table) {}

  // Notification Events
  public record BeforeSendNotificationEvent(
      String catalogName,
      Namespace namespace,
      String table,
      NotificationRequest notificationRequest) {}

  // TODO: Add result once SendNotification API changes are confirmed to return the result.
  public record AfterSendNotificationEvent(String catalogName, Namespace namespace, String table) {}

  // Configuration Events
  public record BeforeGetConfigEvent(String warehouse) {}

  public record AfterGetConfigEvent(ConfigResponse configResponse) {}

  // Legacy events
  public record BeforeCommitTableEvent(
      String catalogName,
      TableIdentifier identifier,
      TableMetadata metadataBefore,
      TableMetadata metadataAfter)
      implements PolarisEvent {}

  public record AfterCommitTableEvent(
      String catalogName,
      TableIdentifier identifier,
      TableMetadata metadataBefore,
      TableMetadata metadataAfter)
      implements PolarisEvent {}

  public record BeforeCommitViewEvent(
      String catalogName,
      TableIdentifier identifier,
      ViewMetadata metadataBefore,
      ViewMetadata metadataAfter)
      implements PolarisEvent {}

  public record AfterCommitViewEvent(
      String catalogName,
      TableIdentifier identifier,
      ViewMetadata metadataBefore,
      ViewMetadata metadataAfter)
      implements PolarisEvent {}

  public record BeforeRefreshTableEvent(String catalogName, TableIdentifier tableIdentifier)
      implements PolarisEvent {}

  public record AfterRefreshTableEvent(String catalogName, TableIdentifier tableIdentifier)
      implements PolarisEvent {}

  public record BeforeRefreshViewEvent(String catalogName, TableIdentifier viewIdentifier)
      implements PolarisEvent {}

  public record AfterRefreshViewEvent(String catalogName, TableIdentifier viewIdentifier)
      implements PolarisEvent {}
}
