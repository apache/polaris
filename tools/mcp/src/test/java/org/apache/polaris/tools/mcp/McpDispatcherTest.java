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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Collections;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

final class McpDispatcherTest {
  private final ObjectMapper mapper = new ObjectMapper();
  private McpDispatcher dispatcher;

  @BeforeEach
  void setUp() {
    dispatcher = new McpDispatcher(mapper, Collections.emptyList(), "polaris-mcp", "dev");
  }

  @Test
  void promptsListReturnsEmptyArray() throws Exception {
    JsonNode request =
        mapper.readTree(
            "{\"jsonrpc\":\"2.0\",\"id\":5,\"method\":\"prompts/list\",\"params\":{}}");
    McpDispatcher.DispatchResult result = dispatcher.handle(request);
    assertThat(result.response()).isNotNull();
    assertThat(result.response().get("result").get("prompts").isArray()).isTrue();
    assertThat(result.response().get("result").get("prompts").size()).isZero();
    assertThat(result.shouldExit()).isFalse();
  }

  @Test
  void resourcesListReturnsEmptyArray() throws Exception {
    JsonNode request =
        mapper.readTree(
            "{\"jsonrpc\":\"2.0\",\"id\":6,\"method\":\"resources/list\",\"params\":{}}");
    McpDispatcher.DispatchResult result = dispatcher.handle(request);
    assertThat(result.response()).isNotNull();
    assertThat(result.response().get("result").get("resources").isArray()).isTrue();
    assertThat(result.response().get("result").get("resources").size()).isZero();
    assertThat(result.shouldExit()).isFalse();
  }
}
