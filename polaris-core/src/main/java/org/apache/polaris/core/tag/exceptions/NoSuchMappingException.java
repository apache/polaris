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
package org.apache.polaris.core.tag.exceptions;

import com.google.errorprone.annotations.FormatMethod;
import jakarta.ws.rs.core.Response;
import org.apache.polaris.core.exceptions.PolarisException;

/** Raised when the requested tag assignment does not exist on the target. */
public class NoSuchMappingException extends PolarisException {

  @FormatMethod
  public NoSuchMappingException(String message, Object... args) {
    super(String.format(message, args));
  }

  @Override
  public int httpStatusCode() {
    return Response.Status.NOT_FOUND.getStatusCode();
  }
}
