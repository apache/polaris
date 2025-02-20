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

package org.apache.polaris.benchmarks

import io.gatling.core.Predef._
import io.gatling.core.structure.ChainBuilder
import io.gatling.http.Predef._
import io.gatling.http.request.builder.HttpRequestBuilder
import org.slf4j.LoggerFactory

object RetryOnHttpCodes {
  private val logger = LoggerFactory.getLogger(getClass)

  implicit class HttpRequestBuilderWithStatusSave(val httpRequestBuilder: HttpRequestBuilder) {
    def saveHttpStatusCode(): HttpRequestBuilder =
      httpRequestBuilder.check(status.saveAs("lastHttpStatusCode"))
  }

  def retryOnHttpStatus(maxRetries: Int, statusCodes: Set[Int], counterName: String = "httpRetry")(
      httpRequestBuilder: HttpRequestBuilder
  ): ChainBuilder =
    exec(session => session.set(counterName, 0))
      .exec(session => session.set("lastHttpStatusCode", -1))
      .asLongAs(session =>
        session(counterName).as[Int] == 0 || (
          session(counterName).as[Int] < maxRetries &&
            statusCodes.contains(session("lastHttpStatusCode").as[Int])
        )
      ) {
        exec(session => session.set(counterName, session(counterName).as[Int] + 1))
          .exec(httpRequestBuilder)
      }
      .doIf(session =>
        session(counterName).as[Int] >= maxRetries && statusCodes.contains(
          session("lastHttpStatusCode").as[Int]
        )
      ) {
        exec { session =>
          logger.warn(
            s"""Max retries (${maxRetries}) attempted for chain "${counterName}". Last HTTP status code: ${session(
                "lastHttpStatusCode"
              ).as[Int]}"""
          )
          session
        }
      }
      .exec(session => session.removeAll("lastHttpStatusCode", counterName))
}
