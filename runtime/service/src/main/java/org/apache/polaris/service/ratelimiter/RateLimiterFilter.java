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
package org.apache.polaris.service.ratelimiter;

import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerRequestFilter;
import jakarta.ws.rs.container.PreMatching;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.Provider;
import java.io.IOException;
import org.apache.polaris.service.config.FilterPriorities;
import org.apache.polaris.service.events.AttributeMap;
import org.apache.polaris.service.events.EventAttributes;
import org.apache.polaris.service.events.PolarisEvent;
import org.apache.polaris.service.events.PolarisEventMetadataFactory;
import org.apache.polaris.service.events.PolarisEventType;
import org.apache.polaris.service.events.listeners.PolarisEventListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Provider
@PreMatching
@Priority(FilterPriorities.RATE_LIMITER_FILTER)
@ApplicationScoped
public class RateLimiterFilter implements ContainerRequestFilter {
  private static final Logger LOGGER = LoggerFactory.getLogger(RateLimiterFilter.class);

  private final RateLimiter rateLimiter;
  private final PolarisEventListener polarisEventListener;
  private final PolarisEventMetadataFactory eventMetadataFactory;

  @Inject
  public RateLimiterFilter(
      RateLimiter rateLimiter,
      PolarisEventListener polarisEventListener,
      PolarisEventMetadataFactory eventMetadataFactory) {
    this.rateLimiter = rateLimiter;
    this.polarisEventListener = polarisEventListener;
    this.eventMetadataFactory = eventMetadataFactory;
  }

  @Override
  public void filter(ContainerRequestContext ctx) throws IOException {
    if (!rateLimiter.canProceed()) {
      polarisEventListener.onEvent(
          new PolarisEvent(
              PolarisEventType.BEFORE_LIMIT_REQUEST_RATE,
              eventMetadataFactory.create(),
              new AttributeMap()
                  .put(EventAttributes.HTTP_METHOD, ctx.getMethod())
                  .put(
                      EventAttributes.REQUEST_URI, ctx.getUriInfo().getAbsolutePath().toString())));
      ctx.abortWith(Response.status(Response.Status.TOO_MANY_REQUESTS).build());
      LOGGER.atDebug().log("Rate limiting request");
    }
  }
}
