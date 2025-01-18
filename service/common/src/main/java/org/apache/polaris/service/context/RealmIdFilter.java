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
package org.apache.polaris.service.context;

import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.NotAuthorizedException;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerRequestFilter;
import jakarta.ws.rs.container.PreMatching;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.Response.Status;
import jakarta.ws.rs.ext.Provider;
import java.util.HashMap;
import java.util.Map;
import org.apache.polaris.core.context.RealmId;
import org.apache.polaris.service.config.PolarisFilterPriorities;

@PreMatching
@ApplicationScoped
@Priority(PolarisFilterPriorities.REALM_ID_FILTER)
@Provider
public class RealmIdFilter implements ContainerRequestFilter {

  public static final String REALM_ID_KEY = "realmId";

  @Inject RealmIdResolver realmIdResolver;

  @Override
  public void filter(ContainerRequestContext rc) {
    RealmId realmId = null;
    try {
      realmId = resolveRealmContext(rc);
    } catch (NotAuthorizedException e) {
      rc.abortWith(Response.status(Status.UNAUTHORIZED).build());
    } catch (Exception e) {
      rc.abortWith(Response.status(Status.INTERNAL_SERVER_ERROR).build());
    }
    rc.setProperty(REALM_ID_KEY, realmId);
  }

  protected RealmId resolveRealmContext(ContainerRequestContext rc) {
    return realmIdResolver.resolveRealmId(
        rc.getUriInfo().getRequestUri().toString(),
        rc.getMethod(),
        rc.getUriInfo().getPath(),
        rc.getHeaders().entrySet().stream()
            .collect(
                HashMap::new, (m, e) -> m.put(e.getKey(), e.getValue().getFirst()), Map::putAll));
  }
}
