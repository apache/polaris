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

final class PolarisPrincipalRoleToolTest {
  private final ObjectMapper mapper = new ObjectMapper();

  @Test
  void listPrincipalRolesHitsManagementEndpoint() throws Exception {
    TestHttpExecutor executor = new TestHttpExecutor(200, "{\"principalRoles\":[]}");
    PolarisPrincipalRoleTool tool =
        new PolarisPrincipalRoleTool(
            mapper, executor, URI.create("http://localhost:8181"), AuthorizationProvider.none());

    ObjectNode args = mapper.createObjectNode();
    args.put("operation", "list");

    tool.call(mapper, args);

    assertThat(executor.lastRequest().method()).isEqualTo("GET");
    assertThat(executor.lastRequest().uri().toString())
        .isEqualTo("http://localhost:8181/api/management/v1/principal-roles");
  }

  @Test
  void assignCatalogRoleRequiresPut() throws Exception {
    TestHttpExecutor executor = new TestHttpExecutor(201, "{}");
    PolarisPrincipalRoleTool tool =
        new PolarisPrincipalRoleTool(
            mapper, executor, URI.create("http://localhost:8181"), AuthorizationProvider.none());

    ObjectNode args = mapper.createObjectNode();
    args.put("operation", "assign-catalog-role");
    args.put("principalRole", "analytics_writer");
    args.put("catalog", "dev");
    ObjectNode body = args.putObject("body");
    body.put("catalogRoleName", "writer");

    tool.call(mapper, args);

    assertThat(executor.lastRequest().method()).isEqualTo("PUT");
    assertThat(executor.lastRequest().uri().toString())
        .isEqualTo(
            "http://localhost:8181/api/management/v1/principal-roles/analytics_writer/catalog-roles/dev");
  }
}
