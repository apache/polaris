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

package org.apache.polaris.service.catalog.generic;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.atomic.AtomicReference;
import org.apache.polaris.service.events.CatalogGenericTableServiceEvents;
import org.apache.polaris.service.events.listeners.PolarisEventListener;
import org.apache.polaris.service.types.LoadGenericTableResponse;
import org.junit.jupiter.api.Test;

class CatalogGenericTableEventServiceDelegatorTest extends AbstractPolarisGenericTableCatalogTest {

  @Test
  void intermediateDataIsAvailableInAfterLoadEvent() {
    AtomicReference<CatalogGenericTableServiceEvents.AfterLoadGenericTableEvent> capturedEvent =
        new AtomicReference<>();

    // Register a listener that captures the after-event
    registerEventListener(
        new PolarisEventListener() {
          @Override
          public void onAfterLoadGenericTable(
              CatalogGenericTableServiceEvents.AfterLoadGenericTableEvent event) {
            capturedEvent.set(event);
          }
        });

    // Trigger the load path using existing test utilities
    loadGenericTable();

    CatalogGenericTableServiceEvents.AfterLoadGenericTableEvent event = capturedEvent.get();
    assertNotNull(event, "Expected AfterLoadGenericTableEvent to be emitted");

    assertTrue(
        event.getIntermediate(LoadGenericTableResponse.class).isPresent(),
        "Expected intermediate LoadGenericTableResponse to be present in after-event");
  }
}
