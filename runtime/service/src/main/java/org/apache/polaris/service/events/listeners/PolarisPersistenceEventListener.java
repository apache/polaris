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

import jakarta.annotation.Nullable;
import java.util.Map;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.polaris.core.entity.PolarisEvent;
import org.apache.polaris.service.events.AfterAttemptTaskEvent;
import org.apache.polaris.service.events.BeforeAttemptTaskEvent;
import org.apache.polaris.service.events.BeforeLimitRequestRateEvent;
import org.apache.polaris.service.events.CatalogsServiceEvents;
import org.apache.polaris.service.events.IcebergRestCatalogEvents;

public abstract class PolarisPersistenceEventListener extends PolarisEventListener {

  // TODO: Ensure all events (except RateLimiter ones) call `processEvent`
  @Override
  public final void onBeforeLimitRequestRate(BeforeLimitRequestRateEvent event) {}

  @Override
  public void onBeforeCommitTable(IcebergRestCatalogEvents.BeforeCommitTableEvent event) {}

  @Override
  public void onAfterCommitTable(IcebergRestCatalogEvents.AfterCommitTableEvent event) {}

  @Override
  public void onBeforeCommitView(IcebergRestCatalogEvents.BeforeCommitViewEvent event) {}

  @Override
  public void onAfterCommitView(IcebergRestCatalogEvents.AfterCommitViewEvent event) {}

  @Override
  public void onBeforeRefreshTable(IcebergRestCatalogEvents.BeforeRefreshTableEvent event) {}

  @Override
  public void onAfterRefreshTable(IcebergRestCatalogEvents.AfterRefreshTableEvent event) {}

  @Override
  public void onBeforeRefreshView(IcebergRestCatalogEvents.BeforeRefreshViewEvent event) {}

  @Override
  public void onAfterRefreshView(IcebergRestCatalogEvents.AfterRefreshViewEvent event) {}

  @Override
  public void onBeforeAttemptTask(BeforeAttemptTaskEvent event) {}

  @Override
  public void onAfterAttemptTask(AfterAttemptTaskEvent event) {}

  @Override
  public void onAfterCreateTable(IcebergRestCatalogEvents.AfterCreateTableEvent event) {
    ContextSpecificInformation contextSpecificInformation = getContextSpecificInformation();
    TableMetadata tableMetadata = event.loadTableResponse().tableMetadata();
    PolarisEvent polarisEvent =
        new PolarisEvent(
            event.catalogName(),
            org.apache.polaris.service.events.PolarisEvent.createEventId(),
            getRequestId(),
            event.getClass().getSimpleName(),
            contextSpecificInformation.timestamp(),
            contextSpecificInformation.principalName(),
            PolarisEvent.ResourceType.TABLE,
            TableIdentifier.of(event.namespace(), event.tableName()).toString());
    Map<String, String> additionalParameters =
        Map.of(
            "table-uuid",
            tableMetadata.uuid(),
            "metadata",
            TableMetadataParser.toJson(tableMetadata));
    polarisEvent.setAdditionalProperties(additionalParameters);
    processEvent(polarisEvent);
  }

  @Override
  public void onAfterCreateCatalog(CatalogsServiceEvents.AfterCreateCatalogEvent event) {
    ContextSpecificInformation contextSpecificInformation = getContextSpecificInformation();
    PolarisEvent polarisEvent =
        new PolarisEvent(
            event.catalog().getName(),
            org.apache.polaris.service.events.PolarisEvent.createEventId(),
            getRequestId(),
            event.getClass().getSimpleName(),
            contextSpecificInformation.timestamp(),
            contextSpecificInformation.principalName(),
            PolarisEvent.ResourceType.CATALOG,
            event.catalog().getName());
    processEvent(polarisEvent);
  }

  protected record ContextSpecificInformation(long timestamp, @Nullable String principalName) {}

  abstract ContextSpecificInformation getContextSpecificInformation();

  @Nullable
  abstract String getRequestId();

  abstract void processEvent(PolarisEvent event);
}
