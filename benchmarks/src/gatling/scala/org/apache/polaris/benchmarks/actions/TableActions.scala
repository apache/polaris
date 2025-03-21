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
import play.api.libs.json.Format.GenericFormat
import play.api.libs.json.OFormat.oFormatFromReadsAndOWrites
import play.api.libs.json.{Format, Json}

import java.util.concurrent.atomic.AtomicReference

/**
 * Actions for performance testing table operations. This class provides methods to create and
 * manage tables within namespaces.
 *
 * @param dp Dataset parameters controlling the dataset generation
 * @param accessToken Reference to the authentication token shared across actions
 * @param maxRetries Maximum number of retry attempts for failed operations
 * @param retryableHttpCodes HTTP status codes that should trigger a retry
 */
case class TableActions(
    dp: DatasetParameters,
    accessToken: AtomicReference[String],
    maxRetries: Int = 10,
    retryableHttpCodes: Set[Int] = Set(409, 500)
) {
  private val logger = LoggerFactory.getLogger(getClass)

  /**
   * Creates a base Gatling Feeder that generates table identities. Each row contains the basic
   * information needed to identify a table: catalog name, namespace path, and table name. Tables
   * are named "T_n" where n is derived from the namespace and table position.
   *
   * @return An iterator providing table identity details
   */
  def tableIdentityFeeder(): Feeder[Any] = dp.nAryTree.lastLevelOrdinals.iterator
    .flatMap { namespaceId =>
      val positionInLevel = namespaceId - dp.nAryTree.lastLevelOrdinals.head
      val parentNamespacePath: Seq[String] = dp.nAryTree
        .pathToRoot(namespaceId)
        .map(ordinal => s"NS_$ordinal")
      Range(0, dp.numTablesPerNs)
        .map { j =>
          val tableId = positionInLevel * dp.numTablesPerNs + j
          Map(
            "catalogName" -> "C_0",
            "parentNamespacePath" -> parentNamespacePath,
            "multipartNamespace" -> parentNamespacePath.mkString("%1F"),
            "tableName" -> s"T_$tableId"
          )
        }
    }
    .take(dp.numTables)

  /**
   * Creates a Gatling Feeder that generates table creation details. Each row builds upon the table
   * identity information of [[TableActions.tableIdentityFeeder]] and adds schema and property
   * details needed for table creation.
   *
   * @return An iterator providing table creation details
   */
  def tableCreationFeeder(): Feeder[Any] = tableIdentityFeeder()
    .map { row =>
      // The field identifiers start at 1 because if they start at 0, they will be overwritten by Iceberg.
      // See https://github.com/apache/iceberg/issues/10084
      val fields: Seq[TableField] = (1 to dp.numColumns)
        .map(id => TableField(id = id, name = s"column$id", `type` = "int", required = true))
      val properties: Map[String, String] = (0 until dp.numTableProperties)
        .map(id => s"InitialAttribute_$id" -> s"$id")
        .toMap
      row ++ Map(
        "schemasType" -> "struct",
        "schemasIdentifierFieldIds" -> "[1]",
        "fieldsStr" -> Json.toJson(fields).toString(),
        "fields" -> fields,
        "initialJsonProperties" -> Json.toJson(properties).toString()
      )
    }

  /**
   * Creates a Gatling Feeder that generates table property updates. Each row contains a single
   * property update targeting a specific table.
   *
   * @return An iterator providing table property update details
   */
  def propertyUpdateFeeder(): Feeder[Any] = tableIdentityFeeder()
    .flatMap(row =>
      Range(0, dp.numTablePropertyUpdates)
        .map(k => row + ("newProperty" -> s"""{"NewAttribute_$k": "NewValue_$k"}"""))
    )

  /**
   * Creates a Gatling Feeder that generates table details. Each row builds upon the table creation
   * information and adds schema expectations for fetch verification. The details should be used to
   * verify the table schema and properties after creation.
   *
   * @return An iterator providing table fetch details with expected response values
   */
  def tableFetchFeeder(): Feeder[Any] = tableIdentityFeeder()
    .map { row =>
      val catalogName: String = row("catalogName").asInstanceOf[String]
      val parentNamespacePath: Seq[String] = row("parentNamespacePath").asInstanceOf[Seq[String]]
      val tableName: String = row("tableName").asInstanceOf[String]
      val initialProperties: Map[String, String] = (0 until dp.numTableProperties)
        .map(id => s"InitialAttribute_$id" -> s"$id")
        .toMap
      row ++ Map(
        "initialProperties" -> initialProperties,
        "location" -> s"${dp.defaultBaseLocation}/$catalogName/${parentNamespacePath.mkString("/")}/$tableName"
      )
    }

  /**
   * Creates a new table in a specified namespace. The table is created with a name, schema
   * definition, and properties that are defined in the [[TableActions.tableCreationFeeder]]. The
   * operation includes retry logic for handling transient failures.
   *
   * There is no limit to the number of users that can create tables concurrently.
   */
  val createTable: ChainBuilder = retryOnHttpStatus(maxRetries, retryableHttpCodes, "Create table")(
    http("Create Table")
      .post("/api/catalog/v1/#{catalogName}/namespaces/#{multipartNamespace}/tables")
      .header("Authorization", "Bearer #{accessToken}")
      .header("Content-Type", "application/json")
      .body(
        StringBody(
          """{
            |  "name": "#{tableName}",
            |  "stage-create": false,
            |  "schema": {
            |    "type": "#{schemasType}",
            |    "fields": #{fieldsStr},
            |    "identifier-field-ids": #{schemasIdentifierFieldIds}
            |  },
            |  "properties": #{initialJsonProperties}
            |}""".stripMargin
        )
      )
      .saveHttpStatusCode()
      .check(status.is(200))
  )

  /**
   * Retrieves details of a specific table by its name and namespace path. The table location,
   * schema, and properties are verified against the values defined in the
   * [[TableActions.tableFetchFeeder]]. This operation validates the table existence and its
   * configuration.
   *
   * There is no limit to the number of users that can fetch tables concurrently.
   */
  val fetchTable: ChainBuilder = exec(
    http("Fetch Table")
      .get("/api/catalog/v1/#{catalogName}/namespaces/#{multipartNamespace}/tables/#{tableName}")
      .header("Authorization", "Bearer #{accessToken}")
      .check(status.is(200))
      .check(jsonPath("$.metadata.table-uuid").saveAs("tableUuid"))
      .check(jsonPath("$.metadata.location").is("#{location}"))
      .check(
        jsonPath("$.metadata.properties")
          .transform(str => EntityProperties.filterMapByPrefix(str, "InitialAttribute_"))
          .is("#{initialProperties}")
      )
  )

  /**
   * Checks if a specific table exists by its name and namespace path.
   */
  val checkTableExists: ChainBuilder = exec(
    http("Check Table Exists")
      .head("/api/catalog/v1/#{catalogName}/namespaces/#{multipartNamespace}/tables/#{tableName}")
      .header("Authorization", "Bearer #{accessToken}")
      .check(status.is(204))
  )

  /**
   * Lists all tables under a specific namespace. This operation retrieves all tables within the
   * given namespace, supporting bulk retrieval of table metadata.
   */
  val fetchAllTables: ChainBuilder = exec(
    http("Fetch all Tables under parent namespace")
      .get("/api/catalog/v1/#{catalogName}/namespaces/#{multipartNamespace}/tables")
      .header("Authorization", "Bearer #{accessToken}")
      .check(status.is(200))
  )

  /**
   * Updates the properties of a specific table by its name and namespace path. The table properties
   * are updated with new values defined in the [[TableActions.propertyUpdateFeeder]].
   *
   * There is no limit to the number of users that can update table properties concurrently.
   */
  val updateTable: ChainBuilder =
    retryOnHttpStatus(maxRetries, retryableHttpCodes, "Update table metadata")(
      http("Update table metadata")
        .post("/api/catalog/v1/#{catalogName}/namespaces/#{multipartNamespace}/tables/#{tableName}")
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
 * This object provides JSON serialization for the table field schema so that it can be used in
 * Gatling response checks.
 */
object TableField {
  implicit val format: Format[TableField] = Json.format[TableField]

  def fromList(json: String): Seq[TableField] = Json.parse(json).as[Seq[TableField]]
}

/**
 * A case class representing a table field schema.
 * @param id Field identifier
 * @param name Field name
 * @param `type` Field type
 * @param required Field requirement
 */
case class TableField(id: Int, name: String, `type`: String, required: Boolean)
