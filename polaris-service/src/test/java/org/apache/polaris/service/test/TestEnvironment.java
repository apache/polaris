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
package org.apache.polaris.service.test;

import jakarta.ws.rs.client.Client;
import java.net.URI;
import java.util.UUID;

/**
 * Defines the test environment that a test should run in.
 *
 * @param apiClient The HTTP client to use when making requests
 * @param baseUri The base URL that requests should target, for example http://localhost:1234
 * @param testId An ID unique to this test. This can be used to prefix resource names, such as
 *     catalog names, to prevent collision.
 */
public record TestEnvironment(Client apiClient, URI baseUri, String testId) {
  public TestEnvironment(Client apiClient, String baseUri) {
    this(apiClient, URI.create(baseUri), UUID.randomUUID().toString().replace("-", ""));
  }
}
