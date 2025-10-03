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
package org.apache.polaris.service.correlation;

import io.smallrye.config.ConfigMapping;

@ConfigMapping(prefix = "polaris.correlation-id")
public interface CorrelationIdConfiguration {

  /**
   * The name of the header that contains the correlation ID.
   *
   * <p>If a request does not contain this header, it will be assigned a new correlation ID
   * generated using the configured {@link #generator()}.
   *
   * <p>All responses will include the correlation ID in this header.
   */
  String headerName();

  /**
   * The correlation ID generator to use, when a request does not contain the {@link #headerName()}.
   */
  Generator generator();

  interface Generator {

    /**
     * The type of the correlation ID generator. Must be a registered {@link CorrelationIdGenerator}
     * identifier.
     */
    String type();
  }
}
