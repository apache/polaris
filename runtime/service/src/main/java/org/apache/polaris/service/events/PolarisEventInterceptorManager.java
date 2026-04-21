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

package org.apache.polaris.service.events;

import io.smallrye.common.annotation.Identifier;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import jakarta.ws.rs.ForbiddenException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** Evaluates configured event interceptors in order for in-flight Polaris events. */
@ApplicationScoped
public class PolarisEventInterceptorManager {
  private final List<PolarisEventInterceptor> interceptors;

  @Inject
  public PolarisEventInterceptorManager(
      PolarisEventInterceptorConfiguration configuration,
      @Any Instance<PolarisEventInterceptor> interceptorBeans) {
    this(resolveInterceptors(configuration, interceptorBeans));
  }

  public PolarisEventInterceptorManager(List<PolarisEventInterceptor> interceptors) {
    this.interceptors = List.copyOf(interceptors);
  }

  private static List<PolarisEventInterceptor> resolveInterceptors(
      PolarisEventInterceptorConfiguration configuration,
      Instance<PolarisEventInterceptor> interceptorBeans) {
    List<PolarisEventInterceptor> resolvedInterceptors = new ArrayList<>();
    for (String interceptorType : configuration.types().orElse(List.of())) {
      resolvedInterceptors.add(
          interceptorBeans.select(Identifier.Literal.of(interceptorType)).get());
    }
    return resolvedInterceptors;
  }

  public boolean hasInterceptors() {
    return !interceptors.isEmpty();
  }

  public PolarisEvent intercept(PolarisEvent event) {
    Objects.requireNonNull(event, "event");

    PolarisEvent effectiveEvent = event;
    for (PolarisEventInterceptor interceptor : interceptors) {
      PolarisEventInterceptor.Result result = interceptor.intercept(effectiveEvent);
      if (result == null) {
        throw new IllegalStateException(
            "Interceptor " + interceptor.getClass().getName() + " returned null result");
      }

      switch (result.action()) {
        case ALLOW -> {
          // no-op
        }
        case DENY -> throw new ForbiddenException(result.reason());
        case MODIFY -> effectiveEvent = result.modifiedEvent();
      }
    }

    return effectiveEvent;
  }
}
