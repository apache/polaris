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

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.spark.sql.SparkSession;

public class BundleSanityChecker {
  public static void main(String[] args) {
    try (SparkSession spark = SparkSession.builder().getOrCreate()) {
      spark.sql("USE polaris");
      spark.sql("CREATE NAMESPACE bundle_ns");
      spark.sql("CREATE TABLE bundle_ns.t (id INT, value STRING) USING ICEBERG");
      spark.sql("INSERT INTO bundle_ns.t VALUES (1, 'a'), (2, 'b')");
      long count = spark.sql("SELECT * FROM bundle_ns.t").count();
      assertThat(count).withFailMessage("Expected 2 rows, got %d", count).isEqualTo(2);
      spark.sql("DROP TABLE bundle_ns.t");
      spark.sql("DROP NAMESPACE bundle_ns");
    } catch (Throwable e) {
      e.printStackTrace();
      System.exit(1);
    }
  }
}
