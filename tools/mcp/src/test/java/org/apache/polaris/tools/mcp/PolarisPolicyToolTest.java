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

final class PolarisPolicyToolTest {
  private final ObjectMapper mapper = new ObjectMapper();

  @Test
  void listPoliciesBuildsExpectedRequest() throws Exception {
    TestHttpExecutor executor = new TestHttpExecutor(200, "{\"policies\":[]}");
    PolarisPolicyTool tool =
        new PolarisPolicyTool(
            mapper, executor, URI.create("http://localhost:8181"), Optional.of("token"));

    ObjectNode args = mapper.createObjectNode();
    args.put("operation", "list");
    args.put("catalog", "dev");
    args.put("namespace", "analytics.daily");

    tool.call(mapper, args);

    assertThat(executor.lastRequest().method()).isEqualTo("GET");
    assertThat(executor.lastRequest().uri().toString())
        .isEqualTo(
            "http://localhost:8181/api/catalog/polaris/v1/dev/namespaces/analytics.daily/policies");
    assertThat(executor.lastRequest().headers().firstValue("Authorization"))
        .contains("Bearer token");
  }

  @Test
  void deletePolicySupportsDetachAllQuery() throws Exception {
    TestHttpExecutor executor = new TestHttpExecutor(204, "");
    PolarisPolicyTool tool =
        new PolarisPolicyTool(
            mapper, executor, URI.create("http://localhost:8181"), Optional.empty());

    ObjectNode args = mapper.createObjectNode();
    args.put("operation", "delete");
    args.put("catalog", "dev");
    args.put("namespace", "analytics.daily");
    args.put("policy", "pii-block");
    ObjectNode query = args.putObject("query");
    query.put("detach-all", "true");

    tool.call(mapper, args);

    assertThat(executor.lastRequest().method()).isEqualTo("DELETE");
    assertThat(executor.lastRequest().uri().toString())
        .isEqualTo(
            "http://localhost:8181/api/catalog/polaris/v1/dev/namespaces/analytics.daily/policies/pii-block?detach-all=true");
  }

  @Test
  void attachPolicyUsesMappingsEndpoint() throws Exception {
    TestHttpExecutor executor = new TestHttpExecutor(204, "");
    PolarisPolicyTool tool =
        new PolarisPolicyTool(
            mapper, executor, URI.create("http://localhost:8181"), Optional.empty());

    ObjectNode args = mapper.createObjectNode();
    args.put("operation", "attach");
    args.put("catalog", "dev");
    args.put("namespace", "analytics.daily");
    args.put("policy", "pii-block");
    ObjectNode body = args.putObject("body");
    body.put("targetType", "TABLE");
    body.put("targetName", "sales");

    tool.call(mapper, args);

    assertThat(executor.lastRequest().method()).isEqualTo("PUT");
    assertThat(executor.lastRequest().uri().toString())
        .isEqualTo(
            "http://localhost:8181/api/catalog/polaris/v1/dev/namespaces/analytics.daily/policies/pii-block/mappings");
    assertThat(executor.lastRequestBody()).contains("\"targetType\":\"TABLE\"");
  }

  @Test
  void applicablePoliciesUsesCatalogEndpoint() throws Exception {
    TestHttpExecutor executor = new TestHttpExecutor(200, "{\"policies\":[]}");
    PolarisPolicyTool tool =
        new PolarisPolicyTool(
            mapper, executor, URI.create("http://localhost:8181"), Optional.empty());

    ObjectNode args = mapper.createObjectNode();
    args.put("operation", "applicable");
    args.put("catalog", "dev");
    ObjectNode query = args.putObject("query");
    query.put("namespace", "analytics.daily");
    query.put("target-name", "sales");

    tool.call(mapper, args);

    assertThat(executor.lastRequest().method()).isEqualTo("GET");
    assertThat(executor.lastRequest().uri().toString())
        .isEqualTo(
            "http://localhost:8181/api/catalog/polaris/v1/dev/applicable-policies?namespace=analytics.daily&target-name=sales");
  }
}
