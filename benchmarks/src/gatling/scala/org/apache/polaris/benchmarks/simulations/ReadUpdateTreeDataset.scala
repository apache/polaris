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
import org.apache.polaris.benchmarks.parameters.{
  ConnectionParameters,
  DatasetParameters,
  WorkloadParameters
}
import org.apache.polaris.benchmarks.parameters.ConnectionParameters.connectionParameters
import org.apache.polaris.benchmarks.parameters.DatasetParameters.datasetParameters
import org.apache.polaris.benchmarks.parameters.WorkloadParameters.workloadParameters
import org.apache.polaris.benchmarks.util.CircularIterator
import org.slf4j.LoggerFactory

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.duration.DurationInt

class ReadUpdateTreeDataset extends Simulation {
  private val logger = LoggerFactory.getLogger(getClass)

  // --------------------------------------------------------------------------------
  // Load parameters
  // --------------------------------------------------------------------------------
  val cp: ConnectionParameters = connectionParameters
  val dp: DatasetParameters = datasetParameters
  val wp: WorkloadParameters = workloadParameters
  cp.explanations.foreach(logger.info)
  dp.explanations.foreach(logger.info)
  wp.explanations.foreach(logger.info)

  // --------------------------------------------------------------------------------
  // Helper values
  // --------------------------------------------------------------------------------
  private val numNamespaces: Int = dp.nAryTree.numberOfNodes
  private val accessToken: AtomicReference[String] = new AtomicReference()

  private val authActions = AuthenticationActions(cp, accessToken)
  private val catActions = CatalogActions(dp, accessToken)
  private val nsActions = NamespaceActions(dp, accessToken)
  private val tblActions = TableActions(dp, accessToken)
  private val viewActions = ViewActions(dp, accessToken)

  // --------------------------------------------------------------------------------
  // Workload: Authenticate and store the access token for later use
  // --------------------------------------------------------------------------------
  val authenticate: ScenarioBuilder = scenario("Authenticate using the OAuth2 REST API endpoint")
    .feed(authActions.feeder())
    .tryMax(5) {
      exec(authActions.authenticateAndSaveAccessToken)
    }

  private val nsListFeeder = new CircularIterator(nsActions.namespaceIdentityFeeder)
  private val nsExistsFeeder = new CircularIterator(nsActions.namespaceIdentityFeeder)
  private val nsFetchFeeder = new CircularIterator(nsActions.namespaceFetchFeeder)
  private val nsUpdateFeeder = new CircularIterator(nsActions.namespacePropertiesUpdateFeeder)

  private val tblListFeeder = new CircularIterator(tblActions.tableIdentityFeeder)
  private val tblExistsFeeder = new CircularIterator(tblActions.tableIdentityFeeder)
  private val tblFetchFeeder = new CircularIterator(tblActions.tableFetchFeeder)
  private val tblUpdateFeeder = new CircularIterator(tblActions.propertyUpdateFeeder)

  private val viewListFeeder = new CircularIterator(viewActions.viewIdentityFeeder)
  private val viewExistsFeeder = new CircularIterator(viewActions.viewIdentityFeeder)
  private val viewFetchFeeder = new CircularIterator(viewActions.viewFetchFeeder)
  private val viewUpdateFeeder = new CircularIterator(viewActions.propertyUpdateFeeder)

  // --------------------------------------------------------------------------------
  // Workload: Randomly read and write entities
  // --------------------------------------------------------------------------------
  val readWriteScenario: ScenarioBuilder =
    scenario("Read and write entities using the Iceberg REST API")
      .exec(authActions.restoreAccessTokenInSession)
      .randomSwitch(
        wp.gatlingReadRatio -> group("Read")(
          uniformRandomSwitch(
            exec(feed(nsListFeeder).exec(nsActions.fetchAllChildrenNamespaces)),
            exec(feed(nsExistsFeeder).exec(nsActions.checkNamespaceExists)),
            exec(feed(nsFetchFeeder).exec(nsActions.fetchNamespace)),
            exec(feed(tblListFeeder).exec(tblActions.fetchAllTables)),
            exec(feed(tblExistsFeeder).exec(tblActions.checkTableExists)),
            exec(feed(tblFetchFeeder).exec(tblActions.fetchTable)),
            exec(feed(viewListFeeder).exec(viewActions.fetchAllViews)),
            exec(feed(viewExistsFeeder).exec(viewActions.checkViewExists)),
            exec(feed(viewFetchFeeder).exec(viewActions.fetchView))
          )
        ),
        wp.gatlingWriteRatio -> group("Write")(
          uniformRandomSwitch(
            exec(feed(nsUpdateFeeder).exec(nsActions.updateNamespaceProperties)),
            exec(feed(tblUpdateFeeder).exec(tblActions.updateTable)),
            exec(feed(viewUpdateFeeder).exec(viewActions.updateView))
          )
        )
      )
}
