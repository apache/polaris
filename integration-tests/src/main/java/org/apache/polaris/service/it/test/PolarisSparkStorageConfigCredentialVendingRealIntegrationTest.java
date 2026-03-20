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

import static org.assertj.core.api.Assertions.assertThat;

import jakarta.ws.rs.core.Response;
import java.nio.file.Path;
import java.util.Map;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.polaris.core.admin.model.AwsStorageConfigInfo;
import org.apache.polaris.core.admin.model.Catalog;
import org.apache.polaris.core.admin.model.CatalogProperties;
import org.apache.polaris.core.admin.model.ExternalCatalog;
import org.apache.polaris.core.admin.model.PolarisCatalog;
import org.apache.polaris.core.admin.model.PrincipalWithCredentials;
import org.apache.polaris.core.admin.model.StorageConfigInfo;
import org.apache.polaris.service.it.env.CatalogApi;
import org.apache.polaris.service.it.env.ClientCredentials;
import org.apache.polaris.service.it.env.IntegrationTestsHelper;
import org.apache.polaris.service.it.env.PolarisApiEndpoints;
import org.apache.polaris.test.rustfs.Rustfs;
import org.apache.polaris.test.rustfs.RustfsAccess;
import org.apache.polaris.test.rustfs.RustfsExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

/**
 * Real credential-vending profile for Spark hierarchy tests.
 *
 * <p>This variant enables subscoped credential vending (no STS shortcut) while reusing the full
 * hierarchy coverage from {@link PolarisSparkStorageConfigCredentialVendingIntegrationTest}.
 */
@ExtendWith(RustfsExtension.class)
public class PolarisSparkStorageConfigCredentialVendingRealIntegrationTest
    extends PolarisSparkStorageConfigCredentialVendingIntegrationTest {

  private static final String RUSTFS_ACCESS_KEY = "foo";
  private static final String RUSTFS_SECRET_KEY = "bar";

  @Rustfs(accessKey = RUSTFS_ACCESS_KEY, secretKey = RUSTFS_SECRET_KEY)
  private RustfsAccess rustfsAccess;

  private Map<String, String> rustfsProps;

  @Override
  @BeforeEach
  public void before(
      PolarisApiEndpoints apiEndpoints, ClientCredentials credentials, @TempDir Path tempDir) {
    endpoints = apiEndpoints;
    client = org.apache.polaris.service.it.env.PolarisClient.polarisClient(endpoints);
    sparkToken = client.obtainToken(credentials);
    managementApi = client.managementApi(sparkToken);
    catalogApi = client.catalogApi(sparkToken);

    warehouseDir = IntegrationTestsHelper.getTemporaryDirectory(tempDir).resolve("spark-warehouse");

    catalogName = client.newEntityName("spark_catalog");
    externalCatalogName = client.newEntityName("spark_ext_catalog");

    rustfsProps = rustfsAccess.icebergProperties();
    String baseLocation = rustfsAccess.s3BucketUri("path/to/data").toString();
    String rustfsEndpoint = rustfsAccess.s3endpoint();

    AwsStorageConfigInfo awsConfigModel =
        AwsStorageConfigInfo.builder()
            .setRoleArn("arn:aws:iam::123456789012:role/my-role")
            .setExternalId("externalId")
            .setUserArn("userArn")
            .setStorageType(StorageConfigInfo.StorageTypeEnum.S3)
            .setAllowedLocations(java.util.List.of(baseLocation))
            .setEndpoint(rustfsEndpoint)
            .setStsEndpoint(rustfsEndpoint)
            .setStsUnavailable(false)
            .build();

    CatalogProperties props = new CatalogProperties(baseLocation);
    props.putAll(rustfsProps);
    props.put("polaris.config.drop-with-purge.enabled", "true");

    Catalog catalog =
        PolarisCatalog.builder()
            .setType(Catalog.TypeEnum.INTERNAL)
            .setName(catalogName)
            .setProperties(props)
            .setStorageConfigInfo(awsConfigModel)
            .build();
    managementApi.createCatalog(catalog);

    CatalogProperties externalProps = new CatalogProperties(baseLocation);
    externalProps.putAll(rustfsProps);
    externalProps.put("polaris.config.drop-with-purge.enabled", "true");

    Catalog externalCatalog =
        ExternalCatalog.builder()
            .setType(Catalog.TypeEnum.EXTERNAL)
            .setName(externalCatalogName)
            .setProperties(externalProps)
            .setStorageConfigInfo(awsConfigModel)
            .build();
    managementApi.createCatalog(externalCatalog);

    spark = buildSparkSession();
    onSpark("USE " + catalogName);
  }

  @Override
  protected Map<String, String> storageEndpointProperties() {
    return rustfsProps == null ? super.storageEndpointProperties() : rustfsProps;
  }

  @Override
  protected boolean isStsUnavailableForHierarchyConfigs() {
    return false;
  }

  @Test
  public void testLoadTableReturnsVendedCredentialsInRealProfile() {
    onSpark("CREATE NAMESPACE realvending");
    onSpark("CREATE TABLE realvending.t1 (id int, data string)");
    onSpark("INSERT INTO realvending.t1 VALUES (1, 'rv1')");

    String principalName = client.newEntityName("real_delegate_user");
    String principalRoleName = client.newEntityName("real_delegate_role");
    PrincipalWithCredentials delegatedPrincipal =
        managementApi.createPrincipalWithRole(principalName, principalRoleName);
    managementApi.makeAdmin(principalRoleName, managementApi.getCatalog(catalogName));
    CatalogApi delegatedCatalogApi = client.catalogApi(client.obtainToken(delegatedPrincipal));

    LoadTableResponse response =
        delegatedCatalogApi.loadTableWithAccessDelegation(
            catalogName, TableIdentifier.of("realvending", "t1"), "ALL");

    assertThat(response.credentials()).isNotEmpty();

    try (Response tableCfg =
        managementApi.getTableStorageConfig(catalogName, "realvending", "t1")) {
      assertThat(tableCfg.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
      AwsStorageConfigInfo effective = tableCfg.readEntity(AwsStorageConfigInfo.class);
      assertThat(effective.getStsUnavailable()).isFalse();
      assertThat(effective.getRoleArn()).isNotBlank();
      assertThat(effective.getStsEndpoint()).isNotBlank();
    }
  }
}
