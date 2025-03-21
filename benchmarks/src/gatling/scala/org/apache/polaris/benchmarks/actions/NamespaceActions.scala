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
import play.api.libs.json.Json

import java.util.concurrent.atomic.AtomicReference

/**
 * Actions for performance testing authentication operations. This class provides methods to
 * authenticate and manage access tokens for API requests.
 *
 * @param dp Dataset parameters controlling the dataset generation
 * @param accessToken Reference to the authentication token shared across actions
 * @param maxRetries Maximum number of retry attempts for failed operations
 * @param retryableHttpCodes HTTP status codes that should trigger a retry
 */
case class NamespaceActions(
    dp: DatasetParameters,
    accessToken: AtomicReference[String],
    maxRetries: Int = 10,
    retryableHttpCodes: Set[Int] = Set(409, 500)
) {
  private val logger = LoggerFactory.getLogger(getClass)

  /**
   * Creates a Gatling Feeder that generates namespace hierarchies. Each row is associated with a
   * distinct namespace. Namespaces are named "NS_n" where n is derived from the n-ary tree
   * position. The feeder provides catalog name, namespace id and namespace path.
   *
   * @return An iterator providing namespace details
   */
  def namespaceIdentityFeeder(): Feeder[Any] = Iterator
    .from(0)
    .map { tableId =>
      val namespaceId = tableId
      val namespacePath: Seq[String] = dp.nAryTree
        .pathToRoot(namespaceId)
        .map(ordinal => s"NS_$ordinal")
      Map(
        "catalogName" -> "C_0",
        "namespaceId" -> tableId,
        "namespacePath" -> namespacePath,
        "namespaceJsonPath" -> Json.toJson(namespacePath).toString(),
        "namespaceMultipartPath" -> namespacePath.mkString("%1F")
      )
    }
    .take(dp.nAryTree.numberOfNodes)

  /**
   * Creates a Gatling Feeder that generates namespace hierarchies. Each row is associated with a
   * distinct namespace and leverages the [[NamespaceActions.namespaceIdentityFeeder()]]. Additional
   * attributes are added to each row, like namespace properties.
   *
   * @return An iterator providing namespace details and their properties
   */
  def namespaceCreationFeeder(): Feeder[Any] = namespaceIdentityFeeder()
    .map { row =>
      val properties: Map[String, String] = (0 until dp.numNamespaceProperties)
        .map(id => s"InitialAttribute_$id" -> s"$id")
        .toMap
      row ++ Map(
        "initialProperties" -> properties,
        "initialJsonProperties" -> Json.toJson(properties).toString()
      )
    }

  /**
   * Creates a Gatling Feeder that generates expected namespace attributes. Each row is associated
   * with a distinct namespace and leverages the [[NamespaceActions.namespaceCreationFeeder]].
   * Additional attributes are added to each row so that the payload returned by the server can be
   * verified. The initial properties from the namespace creation feeder are used to verify the
   * namespace properties.
   *
   * @return An iterator providing namespace details and their properties
   */
  def namespaceFetchFeeder(): Feeder[Any] = namespaceCreationFeeder()
    .map { row =>
      val catalogName = row("catalogName").asInstanceOf[String]
      val namespaceUnixPath = row("namespacePath").asInstanceOf[Seq[String]].mkString("/")
      val location = Map(
        "location" -> s"${dp.defaultBaseLocation}/$catalogName/$namespaceUnixPath"
      )
      row ++ Map(
        "location" -> location
      )
    }

  def namespacePropertiesUpdateFeeder(): Feeder[Any] = namespaceIdentityFeeder()
    .flatMap { row =>
      (0 until dp.numNamespacePropertyUpdates).map { updateId =>
        val updates = Map(s"UpdatedAttribute_$updateId" -> s"$updateId")
        row ++ Map(
          "jsonPropertyUpdates" -> Json.toJson(updates).toString()
        )
      }
    }

  /**
   * Creates a new namespace in a specified catalog. The namespace is created with a full path and
   * properties that are defined in the [[NamespaceActions.namespaceCreationFeeder]].
   *
   * Namespaces have a dependency on the existence of their parent namespaces. As a result, the
   * namespace creation operation is expected to fail if too many concurrent users are run. It is
   * possible that a user tries to create a namespace for which the parent has not been fully
   * created yet.
   *
   * Therefore, the number of concurrent users should start with 1 and increase gradually.
   * Typically, start 1 user and increase by 1 user every second until some arbitrary maximum value.
   */
  val createNamespace: ChainBuilder =
    retryOnHttpStatus(maxRetries, retryableHttpCodes, "Create namespace")(
      http("Create Namespace")
        .post("/api/catalog/v1/#{catalogName}/namespaces")
        .header("Authorization", "Bearer #{accessToken}")
        .header("Content-Type", "application/json")
        .body(
          StringBody(
            """{
              |  "namespace": #{namespaceJsonPath},
              |  "properties": #{initialJsonProperties}
              |}""".stripMargin
          )
        )
        .saveHttpStatusCode()
        .check(status.is(200))
        .check(jsonPath("$.namespace").is("#{namespaceJsonPath}"))
    )

  /**
   * Retrieves details of a specific namespace by its path. The namespace path and properties are
   * verified against the values defined in the [[NamespaceActions.namespaceFetchFeeder]]. This
   * operation validates the namespace existence and its configuration.
   *
   * There is no limit to the number of users that can fetch namespaces concurrently.
   */
  val fetchNamespace: ChainBuilder = exec(
    http("Fetch Namespace")
      .get("/api/catalog/v1/#{catalogName}/namespaces/#{namespaceMultipartPath}")
      .header("Authorization", "Bearer #{accessToken}")
      .check(status.is(200))
      .check(jsonPath("$.namespace").is("#{namespaceJsonPath}"))
      .check(
        jsonPath("$.properties")
          .transform(str => EntityProperties.filterMapByPrefix(str, "InitialAttribute_"))
          .is("#{initialProperties}")
      )
      .check(
        jsonPath("$.properties")
          .transform(str => EntityProperties.filterMapByPrefix(str, "location"))
          .is("#{location}")
      )
  )

  /**
   * Checks if a specific namespace exists by its path.
   */
  val checkNamespaceExists: ChainBuilder = exec(
    http("Check Namespace Exists")
      .head("/api/catalog/v1/#{catalogName}/namespaces/#{namespaceMultipartPath}")
      .header("Authorization", "Bearer #{accessToken}")
      .check(status.is(204))
  )

  /**
   * Lists all child namespaces under a specific parent namespace. This operation retrieves the
   * immediate children of a given namespace, supporting the hierarchical nature of the namespace
   * structure.
   */
  val fetchAllChildrenNamespaces: ChainBuilder = exec(
    http("Fetch all Namespaces under specific parent")
      .get("/api/catalog/v1/#{catalogName}/namespaces?parent=#{namespaceMultipartPath}")
      .header("Authorization", "Bearer #{accessToken}")
      .check(status.is(200))
  )

  val updateNamespaceProperties: ChainBuilder =
    retryOnHttpStatus(maxRetries, retryableHttpCodes, "Update namespace properties")(
      http("Update Namespace Properties")
        .post("/api/catalog/v1/#{catalogName}/namespaces/#{namespaceMultipartPath}/properties")
        .header("Authorization", "Bearer #{accessToken}")
        .header("Content-Type", "application/json")
        .body(
          StringBody(
            """{
              |  "removals": [],
              |  "updates": #{jsonPropertyUpdates}
              |}""".stripMargin
          )
        )
        .saveHttpStatusCode()
        .check(status.is(200))
    )
}
