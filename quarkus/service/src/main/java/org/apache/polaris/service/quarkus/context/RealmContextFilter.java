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
package org.apache.polaris.service.quarkus.context;

import io.smallrye.mutiny.Uni;
import io.vertx.mutiny.core.Vertx;
import jakarta.inject.Inject;
import jakarta.ws.rs.container.ContainerRequestContext;
import org.apache.polaris.service.config.PolarisFilterPriorities;
import org.apache.polaris.service.context.RealmContextResolver;
import org.jboss.resteasy.reactive.server.ServerRequestFilter;

public class RealmContextFilter {

  public static final String REALM_CONTEXT_KEY = "realmContext";

  @Inject RealmContextResolver realmContextResolver;
  @Inject Vertx vertx;

  @ServerRequestFilter(preMatching = true, priority = PolarisFilterPriorities.REALM_CONTEXT_FILTER)
  public Uni<Void> resolveRealmContext(ContainerRequestContext rc) {
    // Note: the default implementation of RealmContextResolver does not block,
    // but since other implementations may, we need to use executeBlocking().
    return vertx
        .executeBlocking(
            () ->
                realmContextResolver.resolveRealmContext(
                    rc.getUriInfo().getRequestUri().toString(),
                    rc.getMethod(),
                    rc.getUriInfo().getPath(),
                    rc.getHeaders()::getFirst))
        .invoke(realmContext -> rc.setProperty(REALM_CONTEXT_KEY, realmContext))
        .replaceWithVoid();
  }
}
