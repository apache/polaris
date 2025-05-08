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

import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.FormatMethod;
import java.io.File;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.DirectoryFileFilter;
import org.apache.commons.io.filefilter.FalseFileFilter;
import org.apache.polaris.service.it.ext.PolarisSparkIntegrationTestBase;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public abstract class SparkIntegrationBase extends PolarisSparkIntegrationTestBase {

  @Override
  protected SparkSession.Builder withCatalog(SparkSession.Builder builder, String catalogName) {
    return builder
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config(
            String.format("spark.sql.catalog.%s", catalogName),
            "org.apache.polaris.spark.SparkCatalog")
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

  @FormatMethod
  protected List<Object[]> sql(String query, Object... args) {
    List<Row> rows = spark.sql(String.format(query, args)).collectAsList();
    if (rows.isEmpty()) {
      return ImmutableList.of();
    }
    return rowsToJava(rows);
  }

  protected List<Object[]> rowsToJava(List<Row> rows) {
    return rows.stream().map(this::toJava).collect(Collectors.toList());
  }

  private Object[] toJava(Row row) {
    return IntStream.range(0, row.size())
        .mapToObj(
            pos -> {
              if (row.isNullAt(pos)) {
                return null;
              }

              Object value = row.get(pos);
              if (value instanceof Row) {
                return toJava((Row) value);
              } else if (value instanceof scala.collection.Seq) {
                return row.getList(pos);
              } else if (value instanceof scala.collection.Map) {
                return row.getJavaMap(pos);
              } else {
                return value;
              }
            })
        .toArray(Object[]::new);
  }

  /** List the name of directories under a given path non-recursively. */
  protected List<String> listDirs(String path) {
    File directory = new File(path);
    return FileUtils.listFilesAndDirs(
            directory, FalseFileFilter.INSTANCE, DirectoryFileFilter.DIRECTORY)
        .stream()
        .map(File::getName)
        .toList();
  }

  /** Generate a string name with given prefix and a random suffix */
  protected String generateName(String prefix) {
    return prefix + "_" + UUID.randomUUID().toString().replaceAll("-", "");
  }
}
