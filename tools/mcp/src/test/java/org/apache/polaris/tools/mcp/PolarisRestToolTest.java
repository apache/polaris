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
import java.util.Optional;
import org.junit.jupiter.api.Test;

final class PolarisRestToolTest {
  private final ObjectMapper mapper = new ObjectMapper();

  @Test
  void callUsesDefaultsAndAuthorization() throws Exception {
    TestHttpExecutor executor = new TestHttpExecutor(200, "{\"status\":\"ok\"}");
    PolarisRestTool tool =
        PolarisRestTool.withPrefix(
            "polaris.management.request",
            "test",
            URI.create("http://localhost:8181"),
            "api/management/v1/",
            mapper,
            executor,
            Optional.of("token-123"));

    ObjectNode args = mapper.createObjectNode();
    args.put("method", "GET");
    args.put("path", "catalogs");
    ObjectNode query = args.putObject("query");
    query.put("includeDisabled", "false");

    ToolExecutionResult result = tool.call(mapper, args);

    assertThat(executor.lastRequest()).isNotNull();
    assertThat(executor.lastRequest().method()).isEqualTo("GET");
    assertThat(executor.lastRequest().uri().toString())
        .isEqualTo("http://localhost:8181/api/management/v1/catalogs?includeDisabled=false");
    assertThat(executor.lastRequest().headers().firstValue("Accept")).contains("application/json");
    assertThat(executor.lastRequest().headers().firstValue("Authorization"))
        .contains("Bearer token-123");
    assertThat(result.isError()).isFalse();
    assertThat(result.metadata().get("status").asInt()).isEqualTo(200);
  }

  @Test
  void callSerializesBodyAndDetectsErrors() throws Exception {
    TestHttpExecutor executor = new TestHttpExecutor(404, "{\"error\":\"missing\"}");
    PolarisRestTool tool =
        PolarisRestTool.general(
            "polaris.rest.request",
            "test",
            URI.create("http://localhost:8181"),
            mapper,
            executor,
            Optional.empty());

    ObjectNode args = mapper.createObjectNode();
    args.put("method", "POST");
    args.put("path", "/api/catalog/v1/oauth/tokens");
    ObjectNode body = args.putObject("body");
    body.put("grant_type", "client_credentials");

    ToolExecutionResult result = tool.call(mapper, args);

    assertThat(executor.lastRequest().method()).isEqualTo("POST");
    assertThat(executor.lastRequest().headers().firstValue("Content-Type"))
        .contains("application/json");
    assertThat(executor.lastRequestBody()).isEqualTo("{\"grant_type\":\"client_credentials\"}");
    assertThat(result.isError()).isTrue();
    assertThat(result.metadata().get("response").get("status").asInt()).isEqualTo(404);
    assertThat(result.text()).contains("Status: 404");
  }
}
