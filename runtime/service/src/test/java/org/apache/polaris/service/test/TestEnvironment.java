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

import java.net.URI;
import java.util.UUID;

/** Defines the test environment that a test should run in. */
public class TestEnvironment {

  private final URI baseUri;
  private final URI baseManagementUri;
  private final String testId;

  public TestEnvironment(String baseUri, String baseManagementUri) {
    this(
        URI.create(baseUri),
        URI.create(baseManagementUri),
        UUID.randomUUID().toString().replace("-", ""));
  }

  public TestEnvironment(URI baseUri, URI baseManagementUri, String testId) {
    this.baseUri = baseUri;
    this.baseManagementUri = baseManagementUri;
    this.testId = testId;
  }

  /** The base URL that requests should target, for example {@code http://localhost:8181} */
  public URI baseUri() {
    return baseUri;
  }

  /**
   * The base URL that requests should target for the management interface, for example {@code
   * http://localhost:8182}
   */
  public URI baseManagementUri() {
    return baseManagementUri;
  }

  /**
   * An ID unique to this test. This can be used to prefix resource names, such as catalog names, to
   * prevent collision.
   */
  public String testId() {
    return testId;
  }
}
