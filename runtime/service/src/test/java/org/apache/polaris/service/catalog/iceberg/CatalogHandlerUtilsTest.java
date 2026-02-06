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

package org.apache.polaris.service.catalog.iceberg;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.iceberg.BaseTable;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.BadRequestException;
import org.apache.iceberg.rest.requests.RegisterTableRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Unit tests for CatalogHandlerUtils.registerTable overwrite support */
public class CatalogHandlerUtilsTest {

  private CatalogHandlerUtils utils;

  @BeforeEach
  public void setUp() {
    // use defaults for retries / rollback flag; fine for these unit tests
    utils = new CatalogHandlerUtils(3, true);
  }

  @AfterEach
  public void tearDown() {
    RegisterTableRequestContext.clear();
  }

  private RegisterTableRequest sampleRequest(String name, String metadataLocation) {
    return new RegisterTableRequest() {
      @Override
      public String name() {
        return name;
      }

      @Override
      public String metadataLocation() {
        return metadataLocation;
      }
    };
  }

  /**
   * When a non-Polaris (non-IcebergCatalog) catalog receives an overwrite=true request and the
   * target table already exists, the operation must be rejected. This prevents accidental
   * replacement of an existing table pointer by catalogs that don't support overwrite semantics.
   */
  @Test
  public void rejectOverwriteNonIcebergExists() {
    Catalog mockCatalog = mock(Catalog.class);
    Namespace ns = Namespace.of("db");
    RegisterTableRequest req = sampleRequest("t", "s3://x/meta.json");
    TableIdentifier ident = TableIdentifier.of(ns, req.name());

    when(mockCatalog.tableExists(ident)).thenReturn(true);

    RegisterTableRequestContext.setOverwrite(true);
    try {
      assertThrows(BadRequestException.class, () -> utils.registerTable(mockCatalog, ns, req));
    } finally {
      RegisterTableRequestContext.clear();
    }

    verify(mockCatalog, never()).registerTable(any(TableIdentifier.class), anyString());
  }

  /**
   * If overwrite=true is set but the non-Polaris catalog reports the table does not exist, fall
   * back to the normal register call (no overwrite semantics available) so the table is created as
   * usual.
   */
  @Test
  public void registerCalledWhenNonIcebergMissing() {
    Catalog mockCatalog = mock(Catalog.class);
    Namespace ns = Namespace.of("db");
    RegisterTableRequest req = sampleRequest("t", "s3://x/meta.json");
    TableIdentifier ident = TableIdentifier.of(ns, req.name());

    when(mockCatalog.tableExists(ident)).thenReturn(false);
    // return a BaseTable so the util method can wrap and return
    BaseTable mockTable = mock(BaseTable.class);
    TableOperations mockOps = mock(TableOperations.class);
    TableMetadata mockMeta = mock(TableMetadata.class);
    when(mockTable.operations()).thenReturn(mockOps);
    when(mockOps.current()).thenReturn(mockMeta);
    when(mockCatalog.registerTable(ident, req.metadataLocation())).thenReturn(mockTable);

    RegisterTableRequestContext.setOverwrite(true);
    try {
      utils.registerTable(mockCatalog, ns, req);
    } finally {
      RegisterTableRequestContext.clear();
    }

    verify(mockCatalog, times(1)).registerTable(ident, req.metadataLocation());
  }

  /**
   * When overwrite=true is requested against our Polaris {@link IcebergCatalog}, always route to
   * the catalog's overwrite-capable overload so the underlying entity is updated atomically.
   */
  @Test
  public void overwriteUsesIcebergRegisterWhenExists() {
    // mock the concrete IcebergCatalog which exposes registerTable(..., overwrite)
    IcebergCatalog iceberg = mock(IcebergCatalog.class);
    Namespace ns = Namespace.of("db");
    RegisterTableRequest req = sampleRequest("t", "s3://x/meta.json");
    TableIdentifier ident = TableIdentifier.of(ns, req.name());

    // Simulate that table exists in the catalog
    when(iceberg.tableExists(ident)).thenReturn(true);
    BaseTable mockTable = mock(BaseTable.class);
    TableOperations mockOps = mock(TableOperations.class);
    TableMetadata mockMeta = mock(TableMetadata.class);
    when(mockTable.operations()).thenReturn(mockOps);
    when(mockOps.current()).thenReturn(mockMeta);
    when(iceberg.registerTable(ident, req.metadataLocation(), true)).thenReturn(mockTable);

    RegisterTableRequestContext.setOverwrite(true);
    try {
      utils.registerTable(iceberg, ns, req);
    } finally {
      RegisterTableRequestContext.clear();
    }

    // verify that the 3-arg overload was invoked; since we mock the class we can verify the call
    verify(iceberg, times(1)).registerTable(ident, req.metadataLocation(), true);
  }

  /**
   * Overwrite requests against {@link IcebergCatalog} should use the three-argument register path
   * regardless of whether the table previously existed, ensuring consistent rewrite behavior.
   */
  @Test
  public void overwriteUsesIcebergRegisterWhenMissing() {
    IcebergCatalog iceberg = mock(IcebergCatalog.class);
    Namespace ns = Namespace.of("db");
    RegisterTableRequest req = sampleRequest("t", "s3://x/meta.json");
    TableIdentifier ident = TableIdentifier.of(ns, req.name());

    when(iceberg.tableExists(ident)).thenReturn(false);
    BaseTable mockTable = mock(BaseTable.class);
    TableOperations mockOps = mock(TableOperations.class);
    TableMetadata mockMeta = mock(TableMetadata.class);
    when(mockTable.operations()).thenReturn(mockOps);
    when(mockOps.current()).thenReturn(mockMeta);
    when(iceberg.registerTable(ident, req.metadataLocation(), true)).thenReturn(mockTable);

    RegisterTableRequestContext.setOverwrite(true);
    try {
      utils.registerTable(iceberg, ns, req);
    } finally {
      RegisterTableRequestContext.clear();
    }

    verify(iceberg, times(1)).registerTable(ident, req.metadataLocation(), true);
  }
}
