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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.net.URI;
import java.util.Optional;
import org.junit.jupiter.api.Test;

final class PolarisTableToolTest {
  private final ObjectMapper mapper = new ObjectMapper();

  @Test
  void listTablesBuildsCorrectRequest() throws Exception {
    TestHttpExecutor executor = new TestHttpExecutor(200, "{\"tables\":[]}");
    PolarisTableTool tool =
        new PolarisTableTool(
            mapper,
            executor,
            URI.create("http://localhost:8181"),
            () -> Optional.of("Bearer token-abc"));

    ObjectNode args = mapper.createObjectNode();
    args.put("operation", "list");
    args.put("catalog", "dev");
    args.put("namespace", "analytics.daily");
    ObjectNode query = args.putObject("query");
    query.put("page-size", "25");

    ToolExecutionResult result = tool.call(mapper, args);

    assertThat(executor.lastRequest().method()).isEqualTo("GET");
    assertThat(executor.lastRequest().uri().toString())
        .isEqualTo(
            "http://localhost:8181/api/catalog/v1/dev/namespaces/analytics.daily/tables?page-size=25");
    assertThat(executor.lastRequest().headers().firstValue("Authorization"))
        .contains("Bearer token-abc");
    assertThat(result.isError()).isFalse();
  }

  @Test
  void getTableSupportsArrayNamespace() throws Exception {
    TestHttpExecutor executor = new TestHttpExecutor(200, "{\"metadata\":{}}");
    PolarisTableTool tool =
        new PolarisTableTool(
            mapper, executor, URI.create("http://localhost:8181"), AuthorizationProvider.none());

    ObjectNode args = mapper.createObjectNode();
    args.put("operation", "GET");
    args.put("catalog", "warehouse");
    ArrayNode namespace = mapper.createArrayNode();
    namespace.add("sales");
    namespace.add("north_america");
    args.set("namespace", namespace);
    args.put("table", "daily_metrics");

    tool.call(mapper, args);

    assertThat(executor.lastRequest().method()).isEqualTo("GET");
    assertThat(executor.lastRequest().uri().toString())
        .isEqualTo(
            "http://localhost:8181/api/catalog/v1/warehouse/namespaces/"
                + "sales.north_america/tables/daily_metrics");
  }

  @Test
  void commitTableRequiresBody() {
    TestHttpExecutor executor = new TestHttpExecutor(200, "{\"status\":\"committed\"}");
    PolarisTableTool tool =
        new PolarisTableTool(
            mapper, executor, URI.create("http://localhost:8181"), AuthorizationProvider.none());

    ObjectNode args = mapper.createObjectNode();
    args.put("operation", "commit");
    args.put("catalog", "warehouse");
    args.put("namespace", "sales");
    args.put("table", "daily_metrics");

    assertThatThrownBy(() -> tool.call(mapper, args))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Commit operations require a request body");
  }

  @Test
  void deleteTableUsesDeleteVerb() throws Exception {
    TestHttpExecutor executor = new TestHttpExecutor(204, "");
    PolarisTableTool tool =
        new PolarisTableTool(
            mapper, executor, URI.create("http://localhost:8181"), AuthorizationProvider.none());

    ObjectNode args = mapper.createObjectNode();
    args.put("operation", "delete");
    args.put("catalog", "warehouse");
    args.put("namespace", "sales");
    args.put("table", "daily_metrics");

    tool.call(mapper, args);

    assertThat(executor.lastRequest().method()).isEqualTo("DELETE");
    assertThat(executor.lastRequest().uri().toString())
        .isEqualTo(
            "http://localhost:8181/api/catalog/v1/warehouse/namespaces/sales/tables/daily_metrics");
  }
}
