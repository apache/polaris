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
package org.apache.polaris.service.http;

import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.Priorities;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerResponseContext;
import jakarta.ws.rs.container.ContainerResponseFilter;
import jakarta.ws.rs.ext.Provider;
import org.apache.http.HttpHeaders;
import org.apache.polaris.version.PolarisVersion;
import org.eclipse.microprofile.config.inject.ConfigProperty;

/** Adds the standard HTTP {@code Server} header to outgoing responses. */
@ApplicationScoped
@Provider
@Priority(Priorities.HEADER_DECORATOR)
public class ServerHeaderFilter implements ContainerResponseFilter {

  static final String SERVER_HEADER_VALUE = "Polaris/" + PolarisVersion.polarisVersionString();

  private final boolean headerEnabled;

  @Inject
  public ServerHeaderFilter(
      @ConfigProperty(name = "polaris.http.version-header.enabled", defaultValue = "false")
          boolean headerEnabled) {
    this.headerEnabled = headerEnabled;
  }

  @Override
  public void filter(
      ContainerRequestContext requestContext, ContainerResponseContext responseContext) {
    if (headerEnabled) {
      responseContext.getHeaders().putSingle(HttpHeaders.SERVER, SERVER_HEADER_VALUE);
    }
  }
}
