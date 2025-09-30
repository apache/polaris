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
package org.apache.polaris.service.tracing;

import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerRequestFilter;
import jakarta.ws.rs.container.PreMatching;
import jakarta.ws.rs.ext.Provider;
import org.apache.polaris.service.config.FilterPriorities;
import org.apache.polaris.service.logging.LoggingConfiguration;

@PreMatching
@ApplicationScoped
@Priority(FilterPriorities.REQUEST_ID_FILTER)
@Provider
public class RequestIdFilter implements ContainerRequestFilter {

  public static final String REQUEST_ID_KEY = "requestId";

  @Inject LoggingConfiguration loggingConfiguration;
  @Inject RequestIdGenerator requestIdGenerator;

  @Override
  public void filter(ContainerRequestContext rc) {
    var requestId = rc.getHeaderString(loggingConfiguration.requestIdHeaderName());
    if (requestId == null) {
      requestId = requestIdGenerator.generateRequestId();
    }
    rc.setProperty(REQUEST_ID_KEY, requestId);
  }
}
