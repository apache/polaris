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
import io.gatling.http.Predef._
import org.apache.polaris.benchmarks.actions.{
  AuthenticationActions,
  CatalogActions,
  NamespaceActions,
  TableActions,
  ViewActions
}
import org.apache.polaris.benchmarks.parameters.ConnectionParameters.connectionParameters
import org.apache.polaris.benchmarks.parameters.DatasetParameters.datasetParameters
import org.slf4j.LoggerFactory

import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}

/**
 * This simulation is a 100% read workload that fetches a tree dataset in Polaris. It is intended to
 * be used against a Polaris instance with a pre-existing tree dataset. It has no side effect on the
 * dataset and therefore can be executed multiple times without any issue.
 */
class ReadTreeDataset extends Simulation {
  private val logger = LoggerFactory.getLogger(getClass)

  // --------------------------------------------------------------------------------
  // Load parameters
  // --------------------------------------------------------------------------------
  private val cp = connectionParameters
  private val dp = datasetParameters
  cp.explanations.foreach(logger.info)
  dp.explanations.foreach(logger.info)

  // --------------------------------------------------------------------------------
  // Helper values
  // --------------------------------------------------------------------------------
  private val numNamespaces: Int = dp.nAryTree.numberOfNodes
  private val accessToken: AtomicReference[String] = new AtomicReference()

  private val authenticationActions = AuthenticationActions(cp, accessToken)
  private val catalogActions = CatalogActions(dp, accessToken)
  private val namespaceActions = NamespaceActions(dp, accessToken)
  private val tableActions = TableActions(dp, accessToken)
  private val viewActions = ViewActions(dp, accessToken)

  private val verifiedCatalogs = new AtomicInteger()
  private val verifiedNamespaces = new AtomicInteger()
  private val verifiedTables = new AtomicInteger()
  private val verifiedViews = new AtomicInteger()

  // --------------------------------------------------------------------------------
  // Workload: Authenticate and store the access token for later use
  // --------------------------------------------------------------------------------
  private val authenticate = scenario("Authenticate using the OAuth2 REST API endpoint")
    .feed(authenticationActions.feeder())
    .tryMax(5) {
      exec(authenticationActions.authenticateAndSaveAccessToken)
    }

  // --------------------------------------------------------------------------------
  // Workload: Verify each catalog
  // --------------------------------------------------------------------------------
  private val verifyCatalogs = scenario("Verify catalogs using the Polaris Management REST API")
    .exec(authenticationActions.restoreAccessTokenInSession)
    .asLongAs(session =>
      verifiedCatalogs.getAndIncrement() < dp.numCatalogs && session.contains("accessToken")
    )(
      feed(catalogActions.feeder())
        .exec(catalogActions.fetchCatalog)
    )

  // --------------------------------------------------------------------------------
  // Workload: Verify namespaces
  // --------------------------------------------------------------------------------
  private val verifyNamespaces = scenario("Verify namespaces using the Iceberg REST API")
    .exec(authenticationActions.restoreAccessTokenInSession)
    .asLongAs(session =>
      verifiedNamespaces.getAndIncrement() < numNamespaces && session.contains("accessToken")
    )(
      feed(namespaceActions.namespaceFetchFeeder())
        .exec(namespaceActions.fetchAllChildrenNamespaces)
        .exec(namespaceActions.checkNamespaceExists)
        .exec(namespaceActions.fetchNamespace)
    )

  // --------------------------------------------------------------------------------
  // Workload: Verify tables
  // --------------------------------------------------------------------------------
  private val verifyTables = scenario("Verify tables using the Iceberg REST API")
    .exec(authenticationActions.restoreAccessTokenInSession)
    .asLongAs(session =>
      verifiedTables.getAndIncrement() < dp.numTables && session.contains("accessToken")
    )(
      feed(tableActions.tableFetchFeeder())
        .exec(tableActions.fetchAllTables)
        .exec(tableActions.checkTableExists)
        .exec(tableActions.fetchTable)
    )

  // --------------------------------------------------------------------------------
  // Workload: Verify views
  // --------------------------------------------------------------------------------
  private val verifyViews = scenario("Verify views using the Iceberg REST API")
    .exec(authenticationActions.restoreAccessTokenInSession)
    .asLongAs(session =>
      verifiedViews.getAndIncrement() < dp.numViews && session.contains("accessToken")
    )(
      feed(viewActions.viewFetchFeeder())
        .exec(viewActions.fetchAllViews)
        .exec(viewActions.checkViewExists)
        .exec(viewActions.fetchView)
    )

  // --------------------------------------------------------------------------------
  // Build up the HTTP protocol configuration and set up the simulation
  // --------------------------------------------------------------------------------
  private val httpProtocol = http
    .baseUrl(cp.baseUrl)
    .acceptHeader("application/json")
    .contentTypeHeader("application/json")

  setUp(
    authenticate
      .inject(atOnceUsers(1))
      .andThen(verifyCatalogs.inject(atOnceUsers(1)))
      .andThen(verifyNamespaces.inject(atOnceUsers(dp.nsDepth)))
      .andThen(verifyTables.inject(atOnceUsers(50)))
      .andThen(verifyViews.inject(atOnceUsers(50)))
  )
    .protocols(httpProtocol)
}
