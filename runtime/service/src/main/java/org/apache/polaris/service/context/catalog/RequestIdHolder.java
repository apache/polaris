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
package org.apache.polaris.service.context.catalog;

import io.quarkus.arc.Unremovable;
import jakarta.annotation.Nullable;
import jakarta.enterprise.context.RequestScoped;
import jakarta.enterprise.inject.Produces;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.polaris.core.context.RequestIdSupplier;

/**
 * Request-scoped holder for the request ID.
 *
 * <p>On HTTP request threads the request ID is set by {@code RequestIdFilter}. On async task
 * threads a new CDI request scope is activated by {@code TaskExecutorImpl}, and the request ID from
 * the originating request is propagated into this holder by {@code RequestIdPropagator}.
 *
 * <p>The holder exposes the stored ID as a {@link RequestIdSupplier} produced bean so that any
 * component injecting {@code Instance<RequestIdSupplier>} can resolve it without depending on
 * JAX-RS internals.
 */
@RequestScoped
public class RequestIdHolder {

  private final AtomicReference<String> requestId = new AtomicReference<>();

  @Nullable
  public String get() {
    return requestId.get();
  }

  /**
   * Produces a {@link RequestIdSupplier} for the current request scope. The returned supplier reads
   * the request ID from this holder at the time it is called, so it reflects any value set after
   * this method is first invoked.
   */
  @Produces
  @RequestScoped
  @Unremovable
  public RequestIdSupplier getRequestIdSupplier() {
    return () -> requestId.get();
  }

  public void set(@Nullable String id) {
    if (!requestId.compareAndSet(null, id)) {
      throw new IllegalStateException("Request ID already set");
    }
  }
}
