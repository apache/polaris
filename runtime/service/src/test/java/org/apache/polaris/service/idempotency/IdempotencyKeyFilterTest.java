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

import static org.apache.polaris.service.idempotency.IdempotencyKeyFilter.IDEMPOTENCY_KEY_HEADER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.core.Response;
import java.time.Duration;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class IdempotencyKeyFilterTest {

  private static final UUID KEY = UUID.fromString("0190f7f4-21d9-7e8b-9c8a-3c4f0a3e8b21");

  @Test
  void validKeyPopulatesContext() {
    IdempotencyRequestContext context = newContext(true);
    ContainerRequestContext request = requestWithHeader(KEY.toString());

    newFilter(true, context).filter(request);

    assertThat(context.pendingKey()).isEqualTo(KEY);
    assertThat(context.pendingExpiry()).isNotNull();
    verify(request, never()).abortWith(any());
  }

  @Test
  void missingHeaderLeavesContextEmpty() {
    IdempotencyRequestContext context = newContext(true);
    ContainerRequestContext request = requestWithHeader(null);

    newFilter(true, context).filter(request);

    assertThat(context.pendingKey()).isNull();
    verify(request, never()).abortWith(any());
  }

  @Test
  void blankHeaderLeavesContextEmpty() {
    IdempotencyRequestContext context = newContext(true);
    ContainerRequestContext request = requestWithHeader("   ");

    newFilter(true, context).filter(request);

    assertThat(context.pendingKey()).isNull();
    verify(request, never()).abortWith(any());
  }

  @Test
  void malformedHeaderAbortsWithBadRequest() {
    IdempotencyRequestContext context = newContext(true);
    ContainerRequestContext request = requestWithHeader("not-a-uuid");

    newFilter(true, context).filter(request);

    assertThat(context.pendingKey()).isNull();
    ArgumentCaptor<Response> aborted = ArgumentCaptor.forClass(Response.class);
    verify(request).abortWith(aborted.capture());
    assertThat(aborted.getValue().getStatus())
        .isEqualTo(Response.Status.BAD_REQUEST.getStatusCode());
  }

  @Test
  void disabledIgnoresHeader() {
    IdempotencyRequestContext context = newContext(false);
    ContainerRequestContext request = requestWithHeader(KEY.toString());

    newFilter(false, context).filter(request);

    assertThat(context.pendingKey()).isNull();
    verify(request, never()).getHeaderString(IDEMPOTENCY_KEY_HEADER);
    verify(request, never()).abortWith(any());
  }

  private static ContainerRequestContext requestWithHeader(String value) {
    ContainerRequestContext request = mock(ContainerRequestContext.class);
    when(request.getHeaderString(IDEMPOTENCY_KEY_HEADER)).thenReturn(value);
    return request;
  }

  private static IdempotencyKeyFilter newFilter(
      boolean enabled, IdempotencyRequestContext context) {
    return new IdempotencyKeyFilter(configuration(enabled), context);
  }

  private static IdempotencyRequestContext newContext(boolean enabled) {
    return new IdempotencyRequestContext(configuration(enabled));
  }

  private static IdempotencyConfiguration configuration(boolean enabled) {
    return new IdempotencyConfiguration() {
      @Override
      public boolean enabled() {
        return enabled;
      }

      @Override
      public Duration ttl() {
        return Duration.ofMinutes(5);
      }
    };
  }
}
