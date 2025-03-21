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

package org.apache.polaris.benchmarks.simulations

import io.gatling.core.Predef._
import io.gatling.core.structure.ScenarioBuilder
import io.gatling.http.Predef._
import org.apache.polaris.benchmarks.actions._
import org.apache.polaris.benchmarks.parameters.ConnectionParameters.connectionParameters
import org.apache.polaris.benchmarks.parameters.DatasetParameters.datasetParameters
import org.apache.polaris.benchmarks.parameters.{ConnectionParameters, DatasetParameters}
import org.slf4j.LoggerFactory

import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}
import scala.concurrent.duration._

/**
 * This simulation is a 100% write workload that creates a tree dataset in Polaris. It is intended
 * to be used against an empty Polaris instance.
 */
class CreateTreeDataset extends Simulation {
  private val logger = LoggerFactory.getLogger(getClass)

  // --------------------------------------------------------------------------------
  // Load parameters
  // --------------------------------------------------------------------------------
  val cp: ConnectionParameters = connectionParameters
  val dp: DatasetParameters = datasetParameters
  cp.explanations.foreach(logger.info)
  dp.explanations.foreach(logger.info)

  // --------------------------------------------------------------------------------
  // Helper values
  // --------------------------------------------------------------------------------
  private val numNamespaces: Int = dp.nAryTree.numberOfNodes
  private val accessToken: AtomicReference[String] = new AtomicReference()

  private val authenticationActions = AuthenticationActions(cp, accessToken, 5, Set(500))
  private val catalogActions = CatalogActions(dp, accessToken, 0, Set())
  private val namespaceActions = NamespaceActions(dp, accessToken, 5, Set(500))
  private val tableActions = TableActions(dp, accessToken, 0, Set())
  private val viewActions = ViewActions(dp, accessToken, 0, Set())

  private val createdCatalogs = new AtomicInteger()
  private val createdNamespaces = new AtomicInteger()
  private val createdTables = new AtomicInteger()
  private val createdViews = new AtomicInteger()

  // --------------------------------------------------------------------------------
  // Workload: Authenticate and store the access token for later use
  // --------------------------------------------------------------------------------
  val authenticate: ScenarioBuilder = scenario("Authenticate using the OAuth2 REST API endpoint")
    .feed(authenticationActions.feeder())
    .exec(authenticationActions.authenticateAndSaveAccessToken)

  // --------------------------------------------------------------------------------
  // Workload: Create catalogs
  // --------------------------------------------------------------------------------
  val createCatalogs: ScenarioBuilder =
    scenario("Create catalogs using the Polaris Management REST API")
      .exec(authenticationActions.restoreAccessTokenInSession)
      .asLongAs(session =>
        createdCatalogs.getAndIncrement() < dp.numCatalogs && session.contains("accessToken")
      )(
        feed(catalogActions.feeder())
          .exec(catalogActions.createCatalog)
      )

  // --------------------------------------------------------------------------------
  // Workload: Create namespaces
  // --------------------------------------------------------------------------------
  val createNamespaces: ScenarioBuilder = scenario("Create namespaces using the Iceberg REST API")
    .exec(authenticationActions.restoreAccessTokenInSession)
    .asLongAs(session =>
      createdNamespaces.getAndIncrement() < numNamespaces && session.contains("accessToken")
    )(
      feed(namespaceActions.namespaceCreationFeeder())
        .exec(namespaceActions.createNamespace)
    )

  // --------------------------------------------------------------------------------
  // Workload: Create tables
  // --------------------------------------------------------------------------------
  val createTables: ScenarioBuilder = scenario("Create tables using the Iceberg REST API")
    .exec(authenticationActions.restoreAccessTokenInSession)
    .asLongAs(session =>
      createdTables.getAndIncrement() < dp.numTables && session.contains("accessToken")
    )(
      feed(tableActions.tableCreationFeeder())
        .exec(tableActions.createTable)
    )

  // --------------------------------------------------------------------------------
  // Workload: Create views
  // --------------------------------------------------------------------------------
  val createViews: ScenarioBuilder = scenario("Create views using the Iceberg REST API")
    .exec(authenticationActions.restoreAccessTokenInSession)
    .asLongAs(session =>
      createdViews.getAndIncrement() < dp.numViews && session.contains("accessToken")
    )(
      feed(viewActions.viewCreationFeeder())
        .exec(viewActions.createView)
    )
}
