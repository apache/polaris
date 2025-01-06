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
package org.apache.polaris.service.dropwizard.exception;

import io.dropwizard.jersey.validation.JerseyViolationException;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.ExceptionMapper;
import jakarta.ws.rs.ext.Provider;
import org.apache.polaris.service.exception.IcebergConstraintViolationExceptionMapper;

/**
 * Override of the default JerseyViolationExceptionMapper to provide an Iceberg ErrorResponse with
 * the exception details.
 */
@Provider
public class JerseyViolationExceptionMapper implements ExceptionMapper<JerseyViolationException> {

  private IcebergConstraintViolationExceptionMapper icebergMapper =
      new IcebergConstraintViolationExceptionMapper();

  @Override
  public Response toResponse(JerseyViolationException exception) {
    return icebergMapper.toResponse(exception);
  }
}
