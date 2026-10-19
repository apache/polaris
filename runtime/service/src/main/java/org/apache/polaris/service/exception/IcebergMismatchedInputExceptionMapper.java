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

import com.fasterxml.jackson.databind.exc.MismatchedInputException;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.ExceptionMapper;
import jakarta.ws.rs.ext.Provider;

/**
 * Routes {@link MismatchedInputException} (a request body that does not match the target model, for
 * example a missing required property or a value of the wrong type) through {@link
 * IcebergJsonProcessingExceptionMapper}.
 *
 * <p>Quarkus ships its own mapper for exactly this exception type, which takes precedence over the
 * Polaris mapper registered for the {@code JsonProcessingException} supertype and answers {@code
 * 400} without a body outside dev and test mode. Registering a mapper for the same type restores
 * the Iceberg error envelope with a message naming the offending property.
 */
@Provider
public class IcebergMismatchedInputExceptionMapper
    implements ExceptionMapper<MismatchedInputException> {
  private final IcebergJsonProcessingExceptionMapper delegate =
      new IcebergJsonProcessingExceptionMapper();

  @Override
  public Response toResponse(MismatchedInputException exception) {
    return delegate.toResponse(exception);
  }
}
