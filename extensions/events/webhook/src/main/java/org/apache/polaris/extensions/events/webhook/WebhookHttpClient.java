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
package org.apache.polaris.extensions.events.webhook;

import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

/**
 * REST client abstraction over the configured webhook endpoint. Instances are built
 * programmatically via {@link org.eclipse.microprofile.rest.client.RestClientBuilder} so that the
 * endpoint remains a runtime configuration value. A {@code null} {@code signature} omits the
 * signature header.
 */
@Path("")
interface WebhookHttpClient {
  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  Response post(
      @HeaderParam(WebhookEventListener.EVENT_TYPE_HEADER) String eventType,
      @HeaderParam(WebhookEventListener.SIGNATURE_HEADER) String signature,
      String payload);
}
