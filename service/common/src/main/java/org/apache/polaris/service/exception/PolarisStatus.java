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
package org.apache.polaris.service.exception;

import jakarta.ws.rs.core.Response;

public enum PolarisStatus implements Response.StatusType {
  AUTHENTICATION_TIMEOUT(419, "Authentication Timeout"),
  ;

  private final int code;
  private final String reason;
  private final Response.Status.Family family;

  PolarisStatus(final int statusCode, final String reasonPhrase) {
    this.code = statusCode;
    this.reason = reasonPhrase;
    this.family = Response.Status.Family.familyOf(statusCode);
  }

  @Override
  public int getStatusCode() {
    return code;
  }

  @Override
  public Response.Status.Family getFamily() {
    return family;
  }

  @Override
  public String getReasonPhrase() {
    return reason;
  }
}
