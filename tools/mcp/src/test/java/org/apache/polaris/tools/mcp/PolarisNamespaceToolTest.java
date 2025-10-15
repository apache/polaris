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
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.net.URI;
import org.junit.jupiter.api.Test;

final class PolarisNamespaceToolTest {
  private final ObjectMapper mapper = new ObjectMapper();

  @Test
  void listNamespacesBuildsExpectedPath() throws Exception {
    TestHttpExecutor executor = new TestHttpExecutor(200, "{\"namespaces\":[]}");
    PolarisNamespaceTool tool =
        new PolarisNamespaceTool(
            mapper, executor, URI.create("http://localhost:8181"), AuthorizationProvider.none());

    ObjectNode args = mapper.createObjectNode();
    args.put("operation", "list");
    args.put("catalog", "dev");

    tool.call(mapper, args);

    assertThat(executor.lastRequest().method()).isEqualTo("GET");
    assertThat(executor.lastRequest().uri().toString())
        .isEqualTo("http://localhost:8181/api/catalog/v1/dev/namespaces");
  }

  @Test
  void createNamespaceUsesBodyAndNamespaceArgument() throws Exception {
    TestHttpExecutor executor = new TestHttpExecutor(200, "{}");
    PolarisNamespaceTool tool =
        new PolarisNamespaceTool(
            mapper, executor, URI.create("http://localhost:8181"), AuthorizationProvider.none());

    ObjectNode args = mapper.createObjectNode();
    args.put("operation", "create");
    args.put("catalog", "dev");
    args.put("namespace", "analytics.daily");
    ObjectNode body = args.putObject("body");
    body.putObject("properties").put("owner", "data-platform");

    tool.call(mapper, args);

    assertThat(executor.lastRequest().method()).isEqualTo("POST");
    assertThat(executor.lastRequest().uri().toString())
        .isEqualTo("http://localhost:8181/api/catalog/v1/dev/namespaces");
    ObjectNode sent = mapper.readTree(executor.lastRequestBody()).deepCopy();
    ArrayNode namespace = (ArrayNode) sent.get("namespace");
    assertThat(namespace).isNotNull();
    assertThat(namespace.get(0).asText()).isEqualTo("analytics");
    assertThat(namespace.get(1).asText()).isEqualTo("daily");
    assertThat(sent.get("properties").get("owner").asText()).isEqualTo("data-platform");
  }

  @Test
  void updatePropertiesTargetsPropertiesEndpoint() throws Exception {
    TestHttpExecutor executor = new TestHttpExecutor(200, "{}");
    PolarisNamespaceTool tool =
        new PolarisNamespaceTool(
            mapper, executor, URI.create("http://localhost:8181"), AuthorizationProvider.none());

    ObjectNode args = mapper.createObjectNode();
    args.put("operation", "update-properties");
    args.put("catalog", "dev");
    args.put("namespace", "analytics.daily");
    ObjectNode body = args.putObject("body");
    body.putObject("updates").put("team", "insights");

    tool.call(mapper, args);

    assertThat(executor.lastRequest().method()).isEqualTo("POST");
    assertThat(executor.lastRequest().uri().toString())
        .isEqualTo(
            "http://localhost:8181/api/catalog/v1/dev/namespaces/analytics.daily/properties");
  }
}
