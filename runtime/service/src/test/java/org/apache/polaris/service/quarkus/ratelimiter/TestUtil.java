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
package org.apache.polaris.service.quarkus.ratelimiter;

import static org.apache.polaris.service.context.TestRealmContextResolver.REALM_PROPERTY_KEY;
import static org.assertj.core.api.Assertions.assertThat;

import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.Response.Status;
import java.util.function.Consumer;
import org.apache.polaris.service.quarkus.test.PolarisIntegrationTestFixture;
import org.apache.polaris.service.quarkus.test.TestEnvironment;

/** Common test utils for testing rate limiting */
public class TestUtil {
  /**
   * Constructs a function that makes a request to list all principal roles and asserts the status
   * of the response. This is a relatively simple type of request that can be used for validating
   * whether the rate limiter intervenes.
   */
  public static Consumer<Status> constructRequestAsserter(
      TestEnvironment testEnv, PolarisIntegrationTestFixture fixture, String realm) {
    return (Response.Status status) -> {
      try (Response response =
          fixture
              .client
              .target(String.format("%s/api/management/v1/principal-roles", testEnv.baseUri()))
              .request("application/json")
              .header("Authorization", "Bearer " + fixture.adminToken)
              .header(REALM_PROPERTY_KEY, realm)
              .get()) {
        assertThat(response).returns(status.getStatusCode(), Response::getStatus);
      }
    };
  }
}
