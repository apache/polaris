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
package org.apache.polaris.service.task;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.context.ContextNotActiveException;
import jakarta.inject.Inject;
import org.apache.polaris.service.context.catalog.RequestIdHolder;
import org.apache.polaris.service.tracing.RequestIdFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

/**
 * Propagates the request ID across the async task boundary.
 *
 * <p>At capture time the request ID is read from {@link RequestIdHolder}, which is populated by
 * {@code RequestIdFilter} on HTTP request threads and by this propagator's {@link
 * RestoreAction#restore()} path on task threads (enabling nested task submission).
 *
 * <p>At restore time the ID is written to both the {@link RequestIdHolder} (so that {@code
 * RequestIdSupplier} works in task threads) and to the SLF4J MDC (so that log messages emitted by
 * the task carry the originating request ID).
 *
 * <p>MDC cleanup is performed by the action's {@link RestoreAction#close()} so that thread-pool
 * threads are left in a clean state after the task completes.
 */
@ApplicationScoped
public class RequestIdPropagator implements AsyncContextPropagator {

  private static final Logger LOGGER = LoggerFactory.getLogger(RequestIdPropagator.class);

  private final RequestIdHolder requestIdHolder;

  @SuppressWarnings("unused") // Required by CDI
  protected RequestIdPropagator() {
    this(null);
  }

  @Inject
  public RequestIdPropagator(RequestIdHolder requestIdHolder) {
    this.requestIdHolder = requestIdHolder;
  }

  @Override
  public RestoreAction capture() {
    String id = null;
    try {
      id = requestIdHolder.get();
    } catch (ContextNotActiveException e) {
      // scope not active
    }
    LOGGER.trace("capture requestId={}", id);
    if (id == null) {
      return RestoreAction.NOOP;
    }
    String captured = id;
    return new RestoreAction() {
      private String previous;

      @Override
      public void restore() {
        LOGGER.trace("restore requestId={}", captured);
        requestIdHolder.set(captured);
        previous = MDC.get(RequestIdFilter.REQUEST_ID_KEY);
        MDC.put(RequestIdFilter.REQUEST_ID_KEY, captured);
      }

      @Override
      public void close() {
        if (previous != null) {
          MDC.put(RequestIdFilter.REQUEST_ID_KEY, previous);
        } else {
          MDC.remove(RequestIdFilter.REQUEST_ID_KEY);
        }
      }
    };
  }
}
