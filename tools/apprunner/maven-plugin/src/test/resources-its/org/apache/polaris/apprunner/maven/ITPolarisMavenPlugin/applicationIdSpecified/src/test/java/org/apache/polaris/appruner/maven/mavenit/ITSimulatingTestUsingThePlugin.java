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
package org.apache.polaris.apprunner.maven.mavenit;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.junit.jupiter.api.Test;
import org.projectnessie.client.api.NessieApiV1;
import org.projectnessie.client.http.HttpClientBuilder;
import org.projectnessie.model.Branch;

/**
 * This is not a test for the plugin itself, this is a test that is run BY the test for the plugin.
 */
class ITSimulatingTestUsingThePlugin {
  @Test
  void pingNessie() throws Exception {
    String port = System.getProperty("quarkus.http.test-port");
    assertNotNull(port, "quarkus.http.test-port");
    String url = System.getProperty("quarkus.http.test-url");
    assertNotNull(url, "quarkus.http.test-url");

    String uri = String.format("http://127.0.0.1:%s/api/v1", port);

    NessieApiV1 client = HttpClientBuilder.builder().withUri(uri).build(NessieApiV1.class);
    // Just some simple REST request to verify that Nessie is started - no fancy interactions w/ Nessie
    client.getConfig();

    // We have seen that HTTP/POST requests can fail with conflicting dependencies
    client.createReference().sourceRefName("main").reference(Branch.of("foo-" + System.nanoTime(), null)).create();
  }
}
