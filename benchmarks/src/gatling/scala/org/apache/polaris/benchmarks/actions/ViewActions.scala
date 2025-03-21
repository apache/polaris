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
import play.api.libs.json.{Format, Json}

import java.time.Instant
import java.util.concurrent.atomic.AtomicReference

case class ViewActions(
    dp: DatasetParameters,
    accessToken: AtomicReference[String],
    maxRetries: Int = 10,
    retryableHttpCodes: Set[Int] = Set(409, 500)
) {
  private val logger = LoggerFactory.getLogger(getClass)

  /**
   * Creates a base Gatling Feeder that generates view identities. Each row contains the basic
   * information needed to identify a view: catalog name, namespace path, and view name.
   *
   * @return An iterator providing view identity details
   */
  def viewIdentityFeeder(): Feeder[Any] = dp.nAryTree.lastLevelOrdinals.iterator
    .flatMap { namespaceId =>
      val catalogId = 0
      val parentNamespacePath: Seq[String] = dp.nAryTree
        .pathToRoot(namespaceId)
        .map(ordinal => s"NS_$ordinal")
      val positionInLevel = namespaceId - dp.nAryTree.lastLevelOrdinals.head

      Range(0, dp.numViewsPerNs)
        .map { j =>
          // Ensure the view ID matches that of the associated table
          val viewId = positionInLevel * dp.numTablesPerNs + j
          Map(
            "catalogName" -> s"C_$catalogId",
            "parentNamespacePath" -> parentNamespacePath,
            "multipartNamespace" -> parentNamespacePath.mkString("%1F"),
            "viewName" -> s"V_$viewId",
            "viewId" -> viewId
          )
        }
    }
    .take(dp.numViews)

  /**
   * Creates a Gatling Feeder that generates view creation details. Each row builds upon the view
   * identity information and adds schema and query details needed for view creation.
   *
   * @return An iterator providing view creation details
   */
  def viewCreationFeeder(): Feeder[Any] = viewIdentityFeeder()
    .map { row =>
      val viewId = row("viewId").asInstanceOf[Int]
      val tableName = s"T_$viewId"
      val fields: Seq[ViewField] = (1 to dp.numColumns)
        .map(id => ViewField(id = id, name = s"column$id", `type` = "int", required = true))
      val properties: Map[String, String] = (0 until dp.numTableProperties)
        .map(id => s"InitialAttribute_$id" -> s"$id")
        .toMap
      row ++ Map(
        "tableName" -> tableName, // Reference the table at the same index as the view
        "timestamp" -> Instant.now().toEpochMilli.toString,
        "fieldsStr" -> Json.toJson(fields).toString(),
        "fields" -> fields,
        "sqlQuery" -> s"SELECT * FROM $tableName",
        "initialJsonProperties" -> Json.toJson(properties).toString()
      )
    }

  /**
   * Creates a Gatling Feeder that generates view property updates. Each row contains a single
   * property update targeting a specific view.
   *
   * @return An iterator providing view property update details
   */
  def propertyUpdateFeeder(): Feeder[Any] = viewIdentityFeeder()
    .flatMap(row =>
      Range(0, dp.numViewPropertyUpdates)
        .map(k => row + ("newProperty" -> s"""{"NewAttribute_$k": "NewValue_$k"}"""))
    )

  /**
   * Creates a Gatling Feeder that generates view details. Each row builds upon the view creation
   * information and adds schema expectations for fetch verification. The details should be used to
   * verify the view schema and properties after creation.
   *
   * @return An iterator providing view fetch details with expected response values
   */
  def viewFetchFeeder(): Feeder[Any] = viewCreationFeeder()
    .map { row =>
      val catalogName: String = row("catalogName").asInstanceOf[String]
      val parentNamespacePath: Seq[String] = row("parentNamespacePath").asInstanceOf[Seq[String]]
      val viewName: String = row("viewName").asInstanceOf[String]
      val initialProperties: Map[String, String] = (0 until dp.numTableProperties)
        .map(id => s"InitialAttribute_$id" -> s"$id")
        .toMap
      row ++ Map(
        "initialProperties" -> initialProperties,
        "location" -> s"${dp.defaultBaseLocation}/$catalogName/${parentNamespacePath.mkString("/")}/$viewName"
      )
    }

  val createView: ChainBuilder = retryOnHttpStatus(maxRetries, retryableHttpCodes, "Create view")(
    http("Create View")
      .post("/api/catalog/v1/#{catalogName}/namespaces/#{multipartNamespace}/views")
      .header("Authorization", "Bearer #{accessToken}")
      .header("Content-Type", "application/json")
      .body(StringBody("""{
                         |  "name": "#{viewName}",
                         |  "view-version": {
                         |    "version-id": 1,
                         |    "timestamp-ms": #{timestamp},
                         |    "schema-id": 0,
                         |    "summary": {
                         |      "engine-version": "3.5.5",
                         |      "app-id": "gatling-#{timestamp}",
                         |      "engine-name": "spark",
                         |      "iceberg-version": "Apache Iceberg 1.7.0"
                         |    },
                         |    "default-namespace": ["#{multipartNamespace}"],
                         |    "representations": [
                         |      {
                         |        "type": "sql",
                         |        "sql": "#{sqlQuery}",
                         |        "dialect": "spark"
                         |      }
                         |    ]
                         |  },
                         |  "schema": {
                         |    "type": "struct",
                         |    "schema-id": 0,
                         |    "fields": #{fieldsStr}
                         |  },
                         | "properties": #{initialJsonProperties}
                         |}""".stripMargin))
      .check(status.is(200))
  )

  val fetchView: ChainBuilder = exec(
    http("Fetch View")
      .get("/api/catalog/v1/#{catalogName}/namespaces/#{multipartNamespace}/views/#{viewName}")
      .header("Authorization", "Bearer #{accessToken}")
      .check(status.is(200))
      .check(jsonPath("$.metadata.view-uuid").saveAs("viewUuid"))
      .check(jsonPath("$.metadata.location").is("#{location}"))
      .check(
        jsonPath("$.metadata.properties")
          .transform(str => EntityProperties.filterMapByPrefix(str, "InitialAttribute_"))
          .is("#{initialProperties}")
      )
  )

  /**
   * Checks if a specific view exists by its name and namespace path.
   */
  val checkViewExists: ChainBuilder = exec(
    http("Check View Exists")
      .head("/api/catalog/v1/#{catalogName}/namespaces/#{multipartNamespace}/views/#{viewName}")
      .header("Authorization", "Bearer #{accessToken}")
      .check(status.is(204))
  )

  val fetchAllViews: ChainBuilder = exec(
    http("Fetch all Views under parent namespace")
      .get("/api/catalog/v1/#{catalogName}/namespaces/#{multipartNamespace}/views")
      .header("Authorization", "Bearer #{accessToken}")
      .check(status.is(200))
  )

  /**
   * Updates the properties of a specific view by its name and namespace path. The view properties
   * are updated with new values defined in the [[ViewActions.propertyUpdateFeeder]].
   *
   * There is no limit to the number of users that can update table properties concurrently.
   */
  val updateView: ChainBuilder =
    retryOnHttpStatus(maxRetries, retryableHttpCodes, "Update View metadata")(
      http("Update View metadata")
        .post("/api/catalog/v1/#{catalogName}/namespaces/#{multipartNamespace}/views/#{viewName}")
        .header("Authorization", "Bearer #{accessToken}")
        .header("Content-Type", "application/json")
        .body(
          StringBody(
            s"""{
               |  "updates": [{
               |    "action": "set-properties",
               |    "updates": #{newProperty}
               |  }]
               |}""".stripMargin
          )
        )
        .saveHttpStatusCode()
        .check(status.is(200).saveAs("lastStatus"))
    )
}

/**
 * This object provides JSON serialization for the view field schema so that it can be used in
 * Gatling response checks.
 */
object ViewField {
  implicit val format: Format[ViewField] = Json.format[ViewField]

  def fromList(json: String): Seq[ViewField] = Json.parse(json).as[Seq[ViewField]]
}

/**
 * A case class representing a view field schema.
 * @param id Field identifier
 * @param name Field name
 * @param `type` Field type
 * @param required Field requirement
 */
case class ViewField(id: Int, name: String, `type`: String, required: Boolean)
