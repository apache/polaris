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

import io.quarkus.vertx.web.RouteFilter;
import io.vertx.ext.web.RoutingContext;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.polaris.core.context.Realm;
import org.slf4j.MDC;

@ApplicationScoped
public class QuarkusLoggingMDCFilter {

  public static final int PRIORITY = RouteFilter.DEFAULT_PRIORITY + 100;

  private static final String REQUEST_ID_KEY = "requestId";
  private static final String REALM_ID_KEY = "realmId";

  @Inject Realm realm;

  @Inject QuarkusLoggingConfiguration loggingConfiguration;

  public static String requestId(RoutingContext rc) {
    return rc.get(REQUEST_ID_KEY);
  }

  public static String realm(RoutingContext rc) {
    return rc.get(REALM_ID_KEY);
  }

  @RouteFilter(value = PRIORITY)
  public void applyMDCContext(RoutingContext rc) {
    // The request scope is active here, so any MDC values set here will be propagated to
    // threads handling the request.
    // Also put the MDC values in the request context for use by other filters and handlers
    loggingConfiguration.mdc().forEach(MDC::put);
    loggingConfiguration.mdc().forEach(rc::put);
    var requestId = rc.request().getHeader(loggingConfiguration.requestIdHeaderName());
    if (requestId != null) {
      MDC.put(REQUEST_ID_KEY, requestId);
      rc.put(REQUEST_ID_KEY, requestId);
    }
    MDC.put(REALM_ID_KEY, realm.id());
    rc.put(REALM_ID_KEY, realm.id());
    // Do not explicitly remove the MDC values from the request context with an end handler,
    // as this could remove MDC context still in use in TaskExecutor threads
    //    rc.addEndHandler(
    //        (v) -> {
    //          MDC.remove(REQUEST_ID_MDC_KEY);
    //          MDC.remove(REALM_ID_MDC_KEY);
    //          loggingConfiguration.mdc().keySet().forEach(MDC::remove);
    //        });
    rc.next();
  }
}
