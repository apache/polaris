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
package org.apache.polaris.service.quarkus.logging;

import static org.apache.polaris.service.quarkus.context.RealmContextFilter.REALM_CONTEXT_KEY;

import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerRequestFilter;
import jakarta.ws.rs.container.PreMatching;
import jakarta.ws.rs.ext.Provider;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.service.quarkus.config.QuarkusFilterPriorities;
import org.slf4j.MDC;

@PreMatching
@ApplicationScoped
@Priority(QuarkusFilterPriorities.MDC_FILTER)
@Provider
public class QuarkusLoggingMDCFilter implements ContainerRequestFilter {

  public static final String REALM_ID_KEY = "realmId";
  public static final String REQUEST_ID_KEY = "requestId";

  @Inject QuarkusLoggingConfiguration loggingConfiguration;

  @Override
  public void filter(ContainerRequestContext rc) {
    // The request scope is active here, so any MDC values set here will be propagated to
    // threads handling the request.
    // Also put the MDC values in the request context for use by other filters and handlers
    loggingConfiguration.mdc().forEach(MDC::put);
    loggingConfiguration.mdc().forEach(rc::setProperty);
    var requestId = rc.getHeaderString(loggingConfiguration.requestIdHeaderName());
    if (requestId != null) {
      MDC.put(REQUEST_ID_KEY, requestId);
      rc.setProperty(REQUEST_ID_KEY, requestId);
    }
    RealmContext realmContext = (RealmContext) rc.getProperty(REALM_CONTEXT_KEY);
    MDC.put(REALM_ID_KEY, realmContext.getRealmIdentifier());
  }
}
