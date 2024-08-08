/*
 * Copyright (c) 2024 Snowflake Computing Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.polaris.service.catalog;

import static org.assertj.core.api.Assertions.assertThat;

import com.adobe.testing.s3mock.testcontainers.S3MockContainer;
import io.dropwizard.testing.ConfigOverride;
import io.dropwizard.testing.ResourceHelpers;
import io.dropwizard.testing.junit5.DropwizardAppExtension;
import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import io.polaris.core.admin.model.AwsStorageConfigInfo;
import io.polaris.core.admin.model.Catalog;
import io.polaris.core.admin.model.CatalogGrant;
import io.polaris.core.admin.model.CatalogPrivilege;
import io.polaris.core.admin.model.CatalogProperties;
import io.polaris.core.admin.model.PolarisCatalog;
import io.polaris.core.admin.model.StorageConfigInfo;
import io.polaris.service.PolarisApplication;
import io.polaris.service.config.PolarisApplicationConfig;
import io.polaris.service.test.PolarisConnectionExtension;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.core.Response;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.Testcontainers;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.TrinoContainer;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.utility.DockerImageName;

@ExtendWith({DropwizardExtensionsSupport.class, PolarisConnectionExtension.class})
public class PolarisTrinoIntegrationTest {
  private static final DropwizardAppExtension<PolarisApplicationConfig> EXT =
      new DropwizardAppExtension<>(
          PolarisApplication.class,
          ResourceHelpers.resourceFilePath("polaris-server-integrationtest.yml"),
          // Bind to random port to support parallelism
          ConfigOverride.config("server.applicationConnectors[0].port", "0"),
          // Bind to random port to support parallelism
          ConfigOverride.config("server.adminConnectors[0].port", "0"));

  private static final String CATALOG_NAME = "mycatalog";
  private static final String S3_HOSTNAME = "s3";

  private static Network network;
  private static S3MockContainer s3 =
      new S3MockContainer("3.9.1").withInitialBuckets("my-bucket,my-old-bucket");
  private static TrinoContainer trino;

  private static PolarisConnectionExtension.PolarisToken polarisToken;

  @BeforeAll
  public static void setup(PolarisConnectionExtension.PolarisToken polarisToken) {
    Testcontainers.exposeHostPorts(EXT.getLocalPort());
    network = Network.newNetwork();

    s3.withNetwork(network);
    s3.withNetworkAliases(S3_HOSTNAME);
    s3.start();

    PolarisTrinoIntegrationTest.polarisToken = polarisToken;

    trino = new TrinoContainer(DockerImageName.parse("trinodb/trino").withTag("453"));
    trino.withNetwork(network);
    trino.withNetworkAliases("trino");
    trino.withAccessToHost(true);
    trino.withCopyToContainer(
        Transferable.of(icebergProperties(CATALOG_NAME)),
        "/etc/trino/catalog/%s.properties".formatted(CATALOG_NAME));
    trino.start();
  }

  @AfterAll
  public static void cleanup() {
    s3.stop();
    network.close();
  }

  @BeforeEach
  public void before() {
    AwsStorageConfigInfo awsConfigModel =
        AwsStorageConfigInfo.builder()
            .setRoleArn("arn:aws:iam::123456789012:role/my-role")
            .setExternalId("externalId")
            .setUserArn("userArn")
            .setStorageType(StorageConfigInfo.StorageTypeEnum.S3)
            .setAllowedLocations(List.of("s3://my-old-bucket/path/to/data"))
            .build();
    CatalogProperties properties = new CatalogProperties("s3://my-bucket/path/to/data");
    properties.putAll(
        Map.of(
            "table-default.s3.endpoint", s3.getHttpEndpoint(),
            "table-default.s3.path-style-access", "true",
            "table-default.s3.access-key-id", "foo",
            "table-default.s3.secret-access-key", "bar",
            "s3.endpoint", s3.getHttpEndpoint(),
            "s3.path-style-access", "true",
            "s3.access-key-id", "foo",
            "s3.secret-access-key", "bar"));
    Catalog catalog =
        PolarisCatalog.builder()
            .setType(Catalog.TypeEnum.INTERNAL)
            .setName(CATALOG_NAME)
            .setProperties(properties)
            .setStorageConfigInfo(awsConfigModel)
            .build();

    try (Response response =
        EXT.client()
            .target(
                String.format("http://localhost:%d/api/management/v1/catalogs", EXT.getLocalPort()))
            .request("application/json")
            .header("Authorization", "BEARER " + polarisToken.token())
            .post(Entity.json(catalog))) {
      assertThat(response).returns(Response.Status.CREATED.getStatusCode(), Response::getStatus);
    }

    CatalogGrant catalogGrant =
        CatalogGrant.builder()
            .setType(CatalogGrant.TypeEnum.CATALOG)
            .setPrivilege(CatalogPrivilege.TABLE_WRITE_DATA)
            .build();
    try (Response response =
        EXT.client()
            .target(
                "http://localhost:%d/api/management/v1/catalogs/%s/catalog-roles/catalog_admin/grants"
                    .formatted(EXT.getLocalPort(), CATALOG_NAME))
            .request("application/json")
            .header("Authorization", "BEARER " + polarisToken.token())
            .put(Entity.json(catalogGrant))) {
      assertThat(response).returns(Response.Status.CREATED.getStatusCode(), Response::getStatus);
    }
  }

  private static String icebergProperties(String catalogName) {
    return """
               connector.name=iceberg
               iceberg.catalog.type=rest
               iceberg.rest-catalog.uri=http://host.testcontainers.internal:%s/api/catalog
               iceberg.rest-catalog.warehouse=%s
               iceberg.rest-catalog.security=OAUTH2
               iceberg.rest-catalog.oauth2.token=%s
               iceberg.rest-catalog.vended-credentials-enabled=true
               fs.hadoop.enabled=false
               fs.native-s3.enabled=true
               s3.region=us-west-2
               s3.path-style-access=true
               s3.endpoint=http://%s:9090
               s3.aws-access-key=foo
               s3.aws-secret-key=bar
               """
        .formatted(EXT.getLocalPort(), catalogName, polarisToken.token(), S3_HOSTNAME);
  }

  @Test
  public void testCreateTable() throws Exception {
    try (Connection connection = DriverManager.getConnection(trino.getJdbcUrl() + CATALOG_NAME);
        Statement statement = connection.createStatement()) {
      ResultSet schemas = statement.executeQuery("SHOW SCHEMAS IN " + CATALOG_NAME);
      assertThat(schemas.next()).isTrue();
      assertThat(schemas.getString(1)).isEqualTo("information_schema");

      statement.execute("CREATE SCHEMA ns1");
      statement.execute("USE ns1");
      statement.execute("CREATE TABLE tb1 (col1 integer, col2 varchar)");
      statement.execute("INSERT INTO tb1 VALUES (1, 'a'), (2, 'b'), (3, 'c')");

      ResultSet result = statement.executeQuery("SELECT count(*) AS cnt FROM tb1");
      assertThat(result.next()).isTrue();
      assertThat(result.getInt(1)).isEqualTo(3);
    }
  }
}
