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

package org.apache.polaris.service.quarkus.config;

import jakarta.enterprise.context.RequestScoped;
import jakarta.ws.rs.container.ContainerRequestContext;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.polaris.core.context.RealmContext;

/**
 * A container for request-scoped information discovered during request execution.
 *
 * <p>This is an equivalent for {@link ContainerRequestContext}, but for use in non-HTTP requests.
 */
@RequestScoped
public class PolarisRequestContext {
  private final AtomicReference<RealmContext> realmCtx = new AtomicReference<>();

  /**
   * Records the {@link RealmContext} that applies to current request. The realm context may be
   * determined from REST API header or by passing explicit realm ID values from one CDI context to
   * another.
   *
   * <p>During the execution of a particular request, this method should be called before {@link
   * #realmContext()}.
   */
  public void setRealmContext(RealmContext rc) {
    realmCtx.set(rc);
  }

  /**
   * Returns the realm context for this request previously set via {@link
   * #setRealmContext(RealmContext)}.
   */
  public RealmContext realmContext() {
    return realmCtx.get();
  }
}
