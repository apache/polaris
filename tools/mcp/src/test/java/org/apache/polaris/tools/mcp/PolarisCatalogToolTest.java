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

package org.apache.polaris.tools.mcp;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.net.URI;
import org.junit.jupiter.api.Test;

final class PolarisCatalogToolTest {
  private final ObjectMapper mapper = new ObjectMapper();

  @Test
  void listCatalogsHitsManagementEndpoint() throws Exception {
    TestHttpExecutor executor = new TestHttpExecutor(200, "{\"catalogs\":[]}");
    PolarisCatalogTool tool =
        new PolarisCatalogTool(
            mapper, executor, URI.create("http://localhost:8181"), AuthorizationProvider.none());

    ObjectNode args = mapper.createObjectNode();
    args.put("operation", "list");

    tool.call(mapper, args);

    assertThat(executor.lastRequest().method()).isEqualTo("GET");
    assertThat(executor.lastRequest().uri().toString())
        .isEqualTo("http://localhost:8181/api/management/v1/catalogs");
  }

  @Test
  void updateCatalogUsesPut() throws Exception {
    TestHttpExecutor executor = new TestHttpExecutor(200, "{}");
    PolarisCatalogTool tool =
        new PolarisCatalogTool(
            mapper, executor, URI.create("http://localhost:8181"), AuthorizationProvider.none());

    ObjectNode args = mapper.createObjectNode();
    args.put("operation", "update");
    args.put("catalog", "manual_spark");
    ObjectNode body = args.putObject("body");
    body.put("version", 3);
    body.put("warehouseLocation", "s3://demo");
    body.put("currentEntityVersion", 2);

    tool.call(mapper, args);

    assertThat(executor.lastRequest().method()).isEqualTo("PUT");
    assertThat(executor.lastRequest().uri().toString())
        .isEqualTo("http://localhost:8181/api/management/v1/catalogs/manual_spark");
  }
}
