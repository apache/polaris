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
package org.apache.polaris.service.it;

import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerRequestFilter;
import jakarta.ws.rs.ext.Provider;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Test-only filter that records, for every request carrying a {@code referenced-by} query
 * parameter, the raw query string as it came over the wire and the value of the parameter after the
 * JAX-RS runtime decoded it, which is the value handed to the server's parser.
 */
@Provider
public class ReferencedByQueryCaptureFilter implements ContainerRequestFilter {

  /**
   * @param rawQuery the query string as received, still percent-encoded
   * @param decodedValue the {@code referenced-by} value as seen by the resource method
   */
  public record Captured(String rawQuery, String decodedValue) {}

  private static final List<Captured> CAPTURED = new CopyOnWriteArrayList<>();

  public static void reset() {
    CAPTURED.clear();
  }

  public static List<Captured> captured() {
    return List.copyOf(CAPTURED);
  }

  @Override
  public void filter(ContainerRequestContext requestContext) {
    String rawQuery = requestContext.getUriInfo().getRequestUri().getRawQuery();
    if (rawQuery != null && rawQuery.contains("referenced-by")) {
      CAPTURED.add(
          new Captured(
              rawQuery,
              requestContext.getUriInfo().getQueryParameters().getFirst("referenced-by")));
    }
  }
}
