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

import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.polaris.core.auth.AuthenticatedPolarisPrincipal;

/** Emitted when Polaris intends to create a table. */
public final class AfterTableCreatedEvent extends PolarisEvent {
  private final String catalogName;
  private final TableIdentifier tableIdentifier;
  private final TableMetadata tableMetadata;
  private final String requestId;
  private final String user;
  private static final org.apache.polaris.core.entity.PolarisEvent.ResourceType resourceType =
      org.apache.polaris.core.entity.PolarisEvent.ResourceType.TABLE;

  public AfterTableCreatedEvent(
      String catalogName,
      TableMetadata metadata,
      String requestId,
      AuthenticatedPolarisPrincipal principal,
      TableIdentifier identifier) {
    this.catalogName = catalogName;
    this.tableIdentifier = identifier;
    this.tableMetadata = metadata;
    this.requestId = requestId;
    if (principal != null) {
      this.user = principal.getName();
    } else {
      this.user = null;
    }
  }

  public String getUser() {
    return user;
  }

  public String getRequestId() {
    return requestId;
  }

  public TableMetadata getTableMetadata() {
    return tableMetadata;
  }

  public TableIdentifier getTableIdentifier() {
    return tableIdentifier;
  }

  public String getCatalogName() {
    return catalogName;
  }

  public org.apache.polaris.core.entity.PolarisEvent.ResourceType getResourceType() {
    return resourceType;
  }
}
