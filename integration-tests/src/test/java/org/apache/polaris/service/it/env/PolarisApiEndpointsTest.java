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
package org.apache.polaris.service.it.env;

import java.net.URI;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Unit tests for PolarisApiEndpoints */
public class PolarisApiEndpointsTest {
  @Test
  void testEndpointRespectsPathPrefix() {
    PolarisApiEndpoints endpoints =
        new PolarisApiEndpoints(URI.create("http://myserver.com/polaris"), "");
    Assertions.assertEquals(
        "http://myserver.com/polaris/api/catalog", endpoints.catalogApiEndpoint().toString());
    Assertions.assertEquals(
        "http://myserver.com/polaris/api/management", endpoints.managementApiEndpoint().toString());
  }
}
