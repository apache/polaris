package org.apache.polaris.service.catalog.generic;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.atomic.AtomicReference;
import org.apache.polaris.service.events.CatalogGenericTableServiceEvents;
import org.apache.polaris.service.events.listeners.PolarisEventListener;
import org.apache.polaris.service.types.LoadGenericTableResponse;
import org.junit.jupiter.api.Test;

class CatalogGenericTableEventServiceDelegatorTest
    extends AbstractPolarisGenericTableCatalogTest {

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
