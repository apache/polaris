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
package org.apache.polaris.service.throttling;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class RequestThrottlingErrorResponse {
  public enum Error {
    request_too_large("The request is too large"),
    ;

    final String errorDescription;

    Error(String errorDescription) {
      this.errorDescription = errorDescription;
    }

    public String getErrorDescription() {
      return errorDescription;
    }
  }

  private final Error error;

  @JsonCreator
  public RequestThrottlingErrorResponse(@JsonProperty("error") Error error) {
    this.error = error;
  }

  public Error getError() {
    return error;
  }
}
