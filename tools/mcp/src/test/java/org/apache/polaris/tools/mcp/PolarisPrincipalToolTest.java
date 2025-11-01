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

final class PolarisPrincipalToolTest {
  private final ObjectMapper mapper = new ObjectMapper();

  @Test
  void createPrincipalTargetsManagementEndpoint() throws Exception {
    TestHttpExecutor executor = new TestHttpExecutor(201, "{}");
    PolarisPrincipalTool tool =
        new PolarisPrincipalTool(
            mapper, executor, URI.create("http://localhost:8181"), AuthorizationProvider.none());

    ObjectNode args = mapper.createObjectNode();
    args.put("operation", "create");
    ObjectNode body = args.putObject("body");
    body.putObject("principal").put("name", "alice");

    tool.call(mapper, args);

    assertThat(executor.lastRequest().method()).isEqualTo("POST");
    assertThat(executor.lastRequest().uri().toString())
        .isEqualTo("http://localhost:8181/api/management/v1/principals");
  }

  @Test
  void assignPrincipalRoleUsesPut() throws Exception {
    TestHttpExecutor executor = new TestHttpExecutor(201, "{}");
    PolarisPrincipalTool tool =
        new PolarisPrincipalTool(
            mapper, executor, URI.create("http://localhost:8181"), AuthorizationProvider.none());

    ObjectNode args = mapper.createObjectNode();
    args.put("operation", "assign-principal-role");
    args.put("principal", "alice");
    ObjectNode body = args.putObject("body");
    body.put("principalRoleName", "analytics_writer");
    body.put("catalogName", "dev");

    tool.call(mapper, args);

    assertThat(executor.lastRequest().method()).isEqualTo("PUT");
    assertThat(executor.lastRequest().uri().toString())
        .isEqualTo("http://localhost:8181/api/management/v1/principals/alice/principal-roles");
  }
}
