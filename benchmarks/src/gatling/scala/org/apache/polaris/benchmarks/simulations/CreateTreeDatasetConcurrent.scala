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
import org.slf4j.LoggerFactory

import scala.concurrent.duration._

/**
 * This simulation is a 100% write workload that creates a tree dataset in Polaris. It is intended
 * to be used against an empty Polaris instance. It is a concurrent version of CreateTreeDataset,
 * i.e. up to 50 requests are sent simultaneously.
 */
class CreateTreeDatasetConcurrent extends CreateTreeDataset {
  private val logger = LoggerFactory.getLogger(getClass)

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
      .andThen(createCatalogs.inject(atOnceUsers(50)))
      .andThen(
        createNamespaces.inject(
          constantUsersPerSec(1).during(1.seconds),
          constantUsersPerSec(dp.nsWidth - 1).during(dp.nsDepth.seconds)
        )
      )
      .andThen(createTables.inject(atOnceUsers(50)))
      .andThen(createViews.inject(atOnceUsers(50)))
  ).protocols(httpProtocol)
}
