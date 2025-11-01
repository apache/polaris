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

import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Objects;

/** Value object representing the result of executing a tool. */
final class ToolExecutionResult {
  private final String text;
  private final boolean error;
  private final ObjectNode metadata;

  ToolExecutionResult(String text, boolean error, ObjectNode metadata) {
    this.text = Objects.requireNonNull(text, "text must not be null");
    this.error = error;
    this.metadata = metadata;
  }

  String text() {
    return text;
  }

  boolean isError() {
    return error;
  }

  ObjectNode metadata() {
    return metadata;
  }
}
