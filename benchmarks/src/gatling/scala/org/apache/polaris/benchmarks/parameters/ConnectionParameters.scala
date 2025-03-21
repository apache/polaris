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

package org.apache.polaris.benchmarks.parameters

/**
 * Case class to hold the connection parameters for the benchmark.
 *
 * @param clientId The client ID for authentication.
 * @param clientSecret The client secret for authentication.
 * @param baseUrl The base URL of the Polaris service.
 */
case class ConnectionParameters(clientId: String, clientSecret: String, baseUrl: String)
    extends Explainable {
  override def explanations: List[String] = List(
    s"The connection parameters point to the server at $baseUrl with client ID $clientId and client secret $clientSecret."
  )
}

/**
 * Object holding the `connectionParameters` instance, populated from environment variables. The
 * `CLIENT_ID` and `CLIENT_SECRET` environment variables must be provided, otherwise authentication
 * will fail. By default, the `BASE_URL` is "http://localhost:8181".
 */
object ConnectionParameters {
  val connectionParameters: ConnectionParameters = ConnectionParameters(
    sys.env("CLIENT_ID"),
    sys.env("CLIENT_SECRET"),
    sys.env.getOrElse("BASE_URL", "http://localhost:8181")
  )
}
