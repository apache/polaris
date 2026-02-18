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
package org.apache.polaris.service.idempotency;

import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Test-only resource that is intentionally outside the configured idempotency scopes, so the
 * idempotency filter should no-op even if an Idempotency-Key is present.
 */
@Path("/test/no-idempotency")
@Consumes(MediaType.TEXT_PLAIN)
@Produces(MediaType.APPLICATION_JSON)
public class IdempotencyOutOfScopeTestResource {

  private static final AtomicInteger COUNTER = new AtomicInteger();

  static void reset() {
    COUNTER.set(0);
  }

  static int count() {
    return COUNTER.get();
  }

  @POST
  public Response post(String body) {
    int n = COUNTER.incrementAndGet();
    return Response.ok("{\"count\":" + n + ",\"body\":\"" + body + "\"}")
        .type(MediaType.APPLICATION_JSON_TYPE)
        .header("X-Test", "out-of-scope")
        .build();
  }
}
