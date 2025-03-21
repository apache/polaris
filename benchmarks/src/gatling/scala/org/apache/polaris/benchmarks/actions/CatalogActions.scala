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

package org.apache.polaris.benchmarks.actions

import io.gatling.core.Predef._
import io.gatling.core.feeder.Feeder
import io.gatling.core.structure.ChainBuilder
import io.gatling.http.Predef._
import org.apache.polaris.benchmarks.RetryOnHttpCodes.{
  retryOnHttpStatus,
  HttpRequestBuilderWithStatusSave
}
import org.apache.polaris.benchmarks.parameters.DatasetParameters
import org.slf4j.LoggerFactory

import java.util.concurrent.atomic.AtomicReference

/**
 * Actions for performance testing catalog operations in Apache Iceberg. This class provides methods
 * to create and fetch catalogs.
 *
 * @param dp Dataset parameters controlling the dataset generation
 * @param accessToken Reference to the authentication token for API requests
 * @param maxRetries Maximum number of retry attempts for failed operations
 * @param retryableHttpCodes HTTP status codes that should trigger a retry
 */
case class CatalogActions(
    dp: DatasetParameters,
    accessToken: AtomicReference[String],
    maxRetries: Int = 10,
    retryableHttpCodes: Set[Int] = Set(409, 500)
) {
  private val logger = LoggerFactory.getLogger(getClass)

  /**
   * Creates a Gatling Feeder that generates catalog names and their default storage locations. Each
   * catalog will be named "C_n" where n is a sequential number, and will have a corresponding
   * storage location under the configured base path.
   *
   * @return An iterator providing catalog names and their storage locations
   */
  def feeder(): Feeder[String] = Iterator
    .from(0)
    .map { i =>
      val catalogName = s"C_$i"
      Map(
        "catalogName" -> catalogName,
        "defaultBaseLocation" -> s"${dp.defaultBaseLocation}/$catalogName"
      )
    }
    .take(dp.numCatalogs)

  /**
   * Creates a new Iceberg catalog with FILE storage type. The catalog is created as an INTERNAL
   * type with a name and a default base location that are defined in the [[CatalogActions.feeder]].
   * This represents the fundamental operation of establishing a new catalog in an Iceberg
   * deployment.
   *
   * There is no limit to the number of users that can create catalogs concurrently.
   */
  val createCatalog: ChainBuilder =
    retryOnHttpStatus(maxRetries, retryableHttpCodes, "Create catalog")(
      http("Create Catalog")
        .post("/api/management/v1/catalogs")
        .header("Authorization", "Bearer #{accessToken}")
        .header("Content-Type", "application/json")
        .body(
          StringBody(
            """{
              |  "catalog": {
              |    "type": "INTERNAL",
              |    "name": "#{catalogName}",
              |    "properties": {
              |      "default-base-location": "#{defaultBaseLocation}"
              |    },
              |    "storageConfigInfo": {
              |      "storageType": "FILE"
              |    }
              |  }
              |}""".stripMargin
          )
        )
        .saveHttpStatusCode()
        .check(status.is(201))
    )

  /**
   * Retrieves details of a specific Iceberg catalog by name. The catalog name is defined in the
   * [[CatalogActions.feeder]]. Some basic properties are verified, like the catalog type, storage
   * settings, and base location.
   *
   * There is no limit to the number of users that can fetch catalogs concurrently.
   */
  val fetchCatalog: ChainBuilder = exec(
    http("Fetch Catalog")
      .get("/api/management/v1/catalogs/#{catalogName}")
      .header("Authorization", "Bearer #{accessToken}")
      .header("Content-Type", "application/json")
      .check(status.is(200))
      .check(jsonPath("$.type").is("INTERNAL"))
      .check(jsonPath("$.name").is("#{catalogName}"))
      .check(jsonPath("$.properties.default-base-location").is("#{defaultBaseLocation}"))
      .check(jsonPath("$.storageConfigInfo.storageType").is("FILE"))
      .check(jsonPath("$.storageConfigInfo.allowedLocations[0]").is("#{defaultBaseLocation}"))
  )

  /**
   * Lists all available Iceberg catalogs in the deployment. This operation does not rely on any
   * feeder data.
   */
  val fetchAllCatalogs: ChainBuilder = exec(
    http("Fetch all Catalogs")
      .get("/api/management/v1/catalogs")
      .header("Authorization", "Bearer #{accessToken}")
      .header("Content-Type", "application/json")
      .check(status.is(200))
  )
}
