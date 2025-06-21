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
package org.apache.polaris.spark.quarkus.it;

import io.quarkus.test.junit.QuarkusIntegrationTest;
import org.apache.spark.sql.SparkSession;

@QuarkusIntegrationTest
public class SparkCatalogIcebergIT extends SparkCatalogBaseIT {
  /** Initialize the spark catalog to use the iceberg spark catalog. */
  @Override
  protected SparkSession.Builder withCatalog(SparkSession.Builder builder, String catalogName) {
    return builder
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config(
            String.format("spark.sql.catalog.%s", catalogName),
            "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.warehouse.dir", warehouseDir.toString())
        .config(String.format("spark.sql.catalog.%s.type", catalogName), "rest")
        .config(
            String.format("spark.sql.catalog.%s.uri", catalogName),
            endpoints.catalogApiEndpoint().toString())
        .config(String.format("spark.sql.catalog.%s.warehouse", catalogName), catalogName)
        .config(String.format("spark.sql.catalog.%s.scope", catalogName), "PRINCIPAL_ROLE:ALL")
        .config(
            String.format("spark.sql.catalog.%s.header.realm", catalogName), endpoints.realmId())
        .config(String.format("spark.sql.catalog.%s.token", catalogName), sparkToken)
        .config(String.format("spark.sql.catalog.%s.s3.access-key-id", catalogName), "fakekey")
        .config(
            String.format("spark.sql.catalog.%s.s3.secret-access-key", catalogName), "fakesecret")
        .config(String.format("spark.sql.catalog.%s.s3.region", catalogName), "us-west-2");
  }
}
