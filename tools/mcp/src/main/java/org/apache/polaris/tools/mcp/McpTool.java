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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

/** Minimal abstraction for a tool exposed through the Model Context Protocol. */
interface McpTool {
  /** Machine readable unique name of the tool. */
  String name();

  /** Short description of what the tool does. */
  String description();

  /**
   * Schema describing the input parameters for the tool, expressed as JSON Schema.
   *
   * @param mapper mapper that can be used to construct JSON nodes
   * @return JSON schema describing the input
   */
  ObjectNode inputSchema(ObjectMapper mapper);

  /**
   * Execute the tool with the supplied arguments.
   *
   * @param mapper mapper to build structured response metadata
   * @param arguments JSON arguments provided by the MCP client
   * @return structured execution result
   * @throws Exception any exception should be surfaced as a tool failure
   */
  ToolExecutionResult call(ObjectMapper mapper, JsonNode arguments) throws Exception;
}
