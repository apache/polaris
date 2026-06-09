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
package org.apache.polaris.service.lineage;

import jakarta.enterprise.context.RequestScoped;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.SecurityContext;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.service.lineage.api.PolarisLineageEvent;
import org.apache.polaris.service.lineage.api.PolarisOpenLineageApiService;

/**
 * No-op implementation of the OpenLineage ingest endpoint.
 *
 * <p>Accepts and discards events. Persistence, dataset resolution, and downstream forwarding will
 * land in follow-up PRs as described in the Polaris OpenLineage proposal.
 */
@RequestScoped
public class OpenLineageAdapter implements PolarisOpenLineageApiService {

  @Override
  public Response sendLineageEvent(
      PolarisLineageEvent event, RealmContext realmContext, SecurityContext securityContext) {
    return Response.status(Response.Status.CREATED).build();
  }
}
