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
import org.apache.iceberg.TableMetadataParser;
import org.apache.polaris.core.entity.PolarisEvent;
import org.apache.polaris.service.events.AfterTableCommitedEvent;
import org.apache.polaris.service.events.AfterTableCreatedEvent;
import org.apache.polaris.service.events.AfterTableRefreshedEvent;
import org.apache.polaris.service.events.AfterTaskAttemptedEvent;
import org.apache.polaris.service.events.AfterViewCommitedEvent;
import org.apache.polaris.service.events.AfterViewRefreshedEvent;
import org.apache.polaris.service.events.BeforeRequestRateLimitedEvent;
import org.apache.polaris.service.events.BeforeTableCommitedEvent;
import org.apache.polaris.service.events.BeforeTableRefreshedEvent;
import org.apache.polaris.service.events.BeforeTaskAttemptedEvent;
import org.apache.polaris.service.events.BeforeViewCommitedEvent;
import org.apache.polaris.service.events.BeforeViewRefreshedEvent;
import org.apache.polaris.service.events.CatalogsServiceEvents;

public abstract class PolarisPersistenceEventListener extends PolarisEventListener {

  // TODO: Ensure all events (except RateLimiter ones) call `addToBuffer`
  @Override
  public final void onBeforeRequestRateLimited(BeforeRequestRateLimitedEvent event) {}

  @Override
  public void onBeforeTableCommited(BeforeTableCommitedEvent event) {}

  @Override
  public void onAfterTableCommited(AfterTableCommitedEvent event) {}

  @Override
  public void onBeforeViewCommited(BeforeViewCommitedEvent event) {}

  @Override
  public void onAfterViewCommited(AfterViewCommitedEvent event) {}

  @Override
  public void onBeforeTableRefreshed(BeforeTableRefreshedEvent event) {}

  @Override
  public void onAfterTableRefreshed(AfterTableRefreshedEvent event) {}

  @Override
  public void onBeforeViewRefreshed(BeforeViewRefreshedEvent event) {}

  @Override
  public void onAfterViewRefreshed(AfterViewRefreshedEvent event) {}

  @Override
  public void onBeforeTaskAttempted(BeforeTaskAttemptedEvent event) {}

  @Override
  public void onAfterTaskAttempted(AfterTaskAttemptedEvent event) {}

  @Override
  public void onAfterTableCreated(AfterTableCreatedEvent event) {
    ContextSpecificInformation contextSpecificInformation = getContextSpecificInformation();
    PolarisEvent polarisEvent =
        new PolarisEvent(
            event.catalogName(),
            org.apache.polaris.service.events.PolarisEvent.createEventId(),
            getRequestId(),
            event.getClass().getSimpleName(),
            contextSpecificInformation.timestamp(),
            contextSpecificInformation.principalName(),
            PolarisEvent.ResourceType.TABLE,
            event.identifier().toString());
    Map<String, String> additionalParameters =
        Map.of(
            "table-uuid",
            event.metadata().uuid(),
            "metadata",
            TableMetadataParser.toJson(event.metadata()));
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

  protected record ContextSpecificInformation(long timestamp, String principalName) {}

  abstract ContextSpecificInformation getContextSpecificInformation();

  abstract String getRequestId();

  abstract void processEvent(PolarisEvent event);
}
