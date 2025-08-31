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
import org.apache.iceberg.catalog.Namespace;
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
      String prefix, CreateNamespaceRequest createNamespaceRequest) {}

  public record AfterCreateNamespaceEvent(
      String prefix, Namespace namespace, Map<String, String> namespaceProperties) {}

  public record BeforeListNamespacesEvent(String prefix, String parent) {}

  public record AfterListNamespacesEvent(String prefix, String parent) {}

  public record BeforeLoadNamespaceMetadataEvent(String prefix, String namespace) {}

  public record AfterLoadNamespaceMetadataEvent(
      String prefix, Namespace namespace, Map<String, String> namespaceProperties) {}

  public record BeforeCheckNamespaceExistsEvent(String prefix, String namespace) {}

  public record AfterCheckNamespaceExistsEvent(String prefix, String namespace) {}

  public record BeforeDropNamespaceEvent(String prefix, String namespace) {}

  public record AfterDropNamespaceEvent(String prefix, String namespace) {}

  public record BeforeUpdateNamespacePropertiesEvent(
      String prefix,
      String namespace,
      UpdateNamespacePropertiesRequest updateNamespacePropertiesRequest) {}

  public record AfterUpdateNamespacePropertiesEvent(
      String prefix,
      String namespace,
      UpdateNamespacePropertiesResponse updateNamespacePropertiesResponse) {}

  // Table Events
  public record BeforeCreateTableEvent(
      String prefix,
      String namespace,
      CreateTableRequest createTableRequest,
      String accessDelegationMode) {}

  public record AfterCreateTableEvent(
      String prefix, String namespace, LoadTableResponse loadTableResponse) {}

  public record BeforeListTablesEvent(String prefix, String namespace) {}

  public record AfterListTablesEvent(String prefix, String namespace) {}

  public record BeforeLoadTableEvent(
      String prefix,
      String namespace,
      String table,
      String accessDelegationMode,
      String ifNoneMatchString,
      String snapshots) {}

  public record AfterLoadTableEvent(
      String prefix, String namespace, LoadTableResponse loadTableResponse) {}

  public record BeforeCheckTableExistsEvent(String prefix, String namespace, String table) {}

  public record AfterCheckTableExistsEvent(String prefix, String namespace, String table) {}

  public record BeforeDropTableEvent(
      String prefix, String namespace, String table, Boolean purgeRequested) {}

  public record AfterDropTableEvent(
      String prefix, String namespace, String table, Boolean purgeRequested) {}

  public record BeforeRegisterTableEvent(
      String prefix, String namespace, RegisterTableRequest registerTableRequest) {}

  public record AfterRegisterTableEvent(
      String prefix, String namespace, LoadTableResponse loadTableResponse) {}

  public record BeforeRenameTableEvent(String prefix, RenameTableRequest renameTableRequest) {}

  public record AfterRenameTableEvent(String prefix, RenameTableRequest renameTableRequest) {}

  public record BeforeUpdateTableEvent(
      String prefix, String namespace, String sourceTable, CommitTableRequest commitTableRequest) {}

  public record AfterUpdateTableEvent(
      String prefix, String namespace, String sourceTable, LoadTableResponse loadTableResponse) {}

  // View Events
  public record BeforeCreateViewEvent(
      String prefix, String namespace, CreateViewRequest createViewRequest) {}

  public record AfterCreateViewEvent(
      String prefix, String namespace, LoadViewResponse loadViewResponse) {}

  public record BeforeListViewsEvent(String prefix, String namespace) {}

  public record AfterListViewsEvent(String prefix, String namespace) {}

  public record BeforeLoadViewEvent(String prefix, String namespace, String view) {}

  public record AfterLoadViewEvent(
      String prefix, String namespace, LoadViewResponse loadViewResponse) {}

  public record BeforeCheckViewExistsEvent(String prefix, String namespace, String view) {}

  public record AfterCheckViewExistsEvent(String prefix, String namespace, String view) {}

  public record BeforeDropViewEvent(String prefix, String namespace, String view) {}

  public record AfterDropViewEvent(String prefix, String namespace, String view) {}

  public record BeforeRenameViewEvent(String prefix, RenameTableRequest renameTableRequest) {}

  public record AfterRenameViewEvent(String prefix, RenameTableRequest renameTableRequest) {}

  public record BeforeReplaceViewEvent(
      String prefix, String namespace, String sourceView, CommitViewRequest commitViewRequest) {}

  public record AfterReplaceViewEvent(
      String prefix, String namespace, String sourceView, LoadViewResponse loadViewResponse) {}

  // Credential Events
  public record BeforeLoadCredentialsEvent(String prefix, String namespace, String table) {}

  public record AfterLoadCredentialsEvent(String prefix, String namespace, String table) {}

  // Notification Events
  public record BeforeSendNotificationEvent(
      String prefix, String namespace, String table, NotificationRequest notificationRequest) {}

  public record AfterSendNotificationEvent(
      String prefix, String namespace, String table, boolean result) {}

  // Configuration Events
  public record BeforeGetConfigEvent(String warehouse) {}

  public record AfterGetConfigEvent(ConfigResponse configResponse) {}
}
