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

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.entity.PolarisEvent;
import org.apache.polaris.service.events.CatalogsServiceEvents;
import org.apache.polaris.service.events.IcebergRestCatalogEvents;

public abstract class PolarisPersistenceEventListener implements PolarisEventListener {

  // TODO: Ensure all events (except RateLimiter ones) call `processEvent`

  @Override
  public void onAfterCreateTable(IcebergRestCatalogEvents.AfterCreateTableEvent event) {
    TableMetadata tableMetadata = event.loadTableResponse().tableMetadata();
    PolarisEvent polarisEvent =
        new PolarisEvent(
            event.catalogName(),
            event.metadata().eventId().toString(),
            event.metadata().requestId().orElse(null),
            event.getClass().getSimpleName(),
            event.metadata().timestamp().toEpochMilli(),
            event.metadata().user().map(PolarisPrincipal::getName).orElse(null),
            PolarisEvent.ResourceType.TABLE,
            TableIdentifier.of(event.namespace(), event.tableName()).toString());
    var additionalParameters =
        ImmutableMap.<String, String>builder()
            .put("table-uuid", tableMetadata.uuid())
            .put("metadata", TableMetadataParser.toJson(tableMetadata));
    additionalParameters.putAll(event.metadata().openTelemetryContext());
    polarisEvent.setAdditionalProperties(additionalParameters.build());
    processEvent(event.metadata().realmId(), polarisEvent);
  }

  @Override
  public void onAfterCreateCatalog(CatalogsServiceEvents.AfterCreateCatalogEvent event) {
    PolarisEvent polarisEvent =
        new PolarisEvent(
            event.catalog().getName(),
            event.metadata().eventId().toString(),
            event.metadata().requestId().orElse(null),
            event.getClass().getSimpleName(),
            event.metadata().timestamp().toEpochMilli(),
            event.metadata().user().map(PolarisPrincipal::getName).orElse(null),
            PolarisEvent.ResourceType.CATALOG,
            event.catalog().getName());
    Map<String, String> openTelemetryContext = event.metadata().openTelemetryContext();
    if (!openTelemetryContext.isEmpty()) {
      polarisEvent.setAdditionalProperties(openTelemetryContext);
    }
    processEvent(event.metadata().realmId(), polarisEvent);
  }

  protected abstract void processEvent(String realmId, PolarisEvent event);
}
