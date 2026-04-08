/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.polaris.persistence.relational.jdbc.models;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;

/**
 * Internal JSON (de)serialization helpers for idempotency record columns.
 *
 * <p>This is intentionally a small, local utility to keep Jackson usage centralized and avoid
 * exposing {@link ObjectMapper} instances as public constants.
 */
public final class IdempotencyRecordSerde {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final TypeReference<Map<String, String>> RESPONSE_HEADERS_TYPE =
      new TypeReference<>() {};

  private IdempotencyRecordSerde() {}

  /**
   * Best-effort parse of the persisted {@code response_headers} JSON.
   *
   * <p>Returns null for blank/malformed values so replay can proceed without headers.
   */
  public static Map<String, String> parseResponseHeaders(String raw) {
    if (raw == null || raw.isBlank()) {
      return null;
    }
    try {
      return MAPPER.readValue(raw, RESPONSE_HEADERS_TYPE);
    } catch (Exception ignored) {
      return null;
    }
  }

  /** Serialize response headers into the persisted {@code response_headers} JSON column. */
  public static String serializeResponseHeaders(Map<String, String> headers) throws Exception {
    if (headers == null || headers.isEmpty()) {
      return null;
    }
    return MAPPER.writeValueAsString(headers);
  }
}
