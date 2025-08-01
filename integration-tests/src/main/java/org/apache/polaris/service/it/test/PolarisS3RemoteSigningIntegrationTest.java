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
package org.apache.polaris.service.it.test;

import static org.apache.polaris.core.storage.StorageAccessProperty.AWS_ENDPOINT;
import static org.apache.polaris.core.storage.StorageAccessProperty.AWS_PATH_STYLE_ACCESS;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.iceberg.io.ResolvingFileIO;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.polaris.core.admin.model.Catalog;
import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.service.it.env.CatalogConfig;
import org.apache.polaris.service.it.env.RestCatalogConfig;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/** Runs PolarisRestCatalogIntegrationBase test on S3 with remote signing enabled. */
@CatalogConfig(properties = {"header.X-Iceberg-Access-Delegation", "remote-signing"})
@RestCatalogConfig({
  // The default client file IO implementation is InMemoryFileIO,
  // which does not support remote signing.
  org.apache.iceberg.CatalogProperties.FILE_IO_IMPL,
  "org.apache.iceberg.io.ResolvingFileIO",
})
public abstract class PolarisS3RemoteSigningIntegrationTest
    extends PolarisRestCatalogAwsIntegrationTest {

  @Override
  protected ImmutableMap.Builder<String, String> clientFileIOProperties() {
    ImmutableMap.Builder<String, String> builder =
        super.clientFileIOProperties()
            .put(AWS_PATH_STYLE_ACCESS.getPropertyName(), String.valueOf(pathStyleAccess()));
    endpoint().ifPresent(endpoint -> builder.put(AWS_ENDPOINT.getPropertyName(), endpoint));
    return builder;
  }

  /**
   * Override this method to return true if the S3 client should use path-style access. Default is
   * false, which means virtual-hosted-style access will be used.
   */
  protected boolean pathStyleAccess() {
    return false;
  }

  @CatalogConfig(properties = {"polaris.config.remote-signing.enabled", "false"})
  @Test
  public void testInternalCatalogRemoteSigningDisabled() {
    @SuppressWarnings("resource")
    RESTCatalog catalog = catalog();
    Namespace ns1 = Namespace.of("ns1");
    catalog.createNamespace(ns1);
    TableIdentifier tableIdentifier = TableIdentifier.of(ns1, "my_table");
    assertThatThrownBy(() -> catalog.createTable(tableIdentifier, SCHEMA))
        .isInstanceOf(ForbiddenException.class)
        .hasMessageContaining("Remote signing is not enabled for this catalog")
        .hasMessageContaining(FeatureConfiguration.REMOTE_SIGNING_ENABLED.key())
        .hasMessageContaining(FeatureConfiguration.REMOTE_SIGNING_ENABLED.catalogConfig());
  }

  @CatalogConfig(Catalog.TypeEnum.EXTERNAL)
  @Test
  public void testExternalCatalogRemoteSigningDisabled() {
    @SuppressWarnings("resource")
    RESTCatalog catalog = catalog();
    Namespace ns1 = Namespace.of("ns1");
    catalog.createNamespace(ns1);
    TableMetadata tableMetadata =
        TableMetadata.newTableMetadata(
            SCHEMA,
            PartitionSpec.unpartitioned(),
            externalCatalogBaseLocation() + "/ns1/my_table",
            Map.of());
    try (ResolvingFileIO resolvingFileIO = initializeClientFileIO(new ResolvingFileIO())) {
      String fileLocation =
          externalCatalogBaseLocation() + "/ns1/my_table/metadata/v1.metadata.json";
      TableMetadataParser.write(tableMetadata, resolvingFileIO.newOutputFile(fileLocation));
      catalog.registerTable(TableIdentifier.of(ns1, "my_table"), fileLocation);
      try {
        assertThatThrownBy(() -> catalog.loadTable(TableIdentifier.of(ns1, "my_table")))
            .isInstanceOf(ForbiddenException.class)
            .hasMessageContaining("Remote signing is not enabled for external catalogs")
            .hasMessageContaining(
                FeatureConfiguration.REMOTE_SIGNING_EXTERNAL_CATALOGS_ENABLED.key());
      } finally {
        resolvingFileIO.deleteFile(fileLocation);
      }
    }
  }

  @Test
  @Override
  @Disabled("It's not possible to request an access delegation mode when registering a table.")
  public void testRegisterTable() {
    // FIXME this test should work if Polaris could send the right AccessConfig even if no
    // delegation mode was requested when registering the table.
  }

  @Test
  @Override
  @Disabled("This test is explicitly for vended credentials")
  public void testLoadTableWithAccessDelegationForExternalCatalogWithConfigDisabled() {}

  @Test
  @Override
  @Disabled("This test is explicitly for vended credentials")
  public void testLoadTableWithoutAccessDelegationForExternalCatalogWithConfigDisabled() {}
}
